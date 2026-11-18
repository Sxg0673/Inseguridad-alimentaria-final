"""
Logica del consumidor: procesar mensajes en micro-lotes y ciclo de consumo.

Por cada mensaje del topic:
  1. Guarda el mensaje tal como llego en stg_evento_encuesta (antes de validar).
  2. Valida esquema y valores con el contrato -> INVALIDO_ESQUEMA / INVALIDO_VALORES.
  3. Resuelve las FK por clave natural en las dimensiones -> INVALIDO_REFERENCIA.
  4. Inserta en fact_evento_encuesta; si el hogar ya estaba -> DUPLICADO.
  5. Actualiza fact_metrica_departamento_stream y marca el crudo como VALIDO.

Micro-lotes: los mensajes que entrega Kafka de una vez (hasta TAMANO_LOTE) se
procesan en UNA transaccion de la base y, despues del commit, se confirman sus
offsets en Kafka (asincrono). Con una transaccion por mensaje, cada commit
esperaba la escritura en disco y el consumidor no pasaba de ~40-90 eventos/s
en Docker para Mac (medido); por lote, ese costo se paga una vez cada N mensajes.

Garantias (las mismas que mensaje por mensaje):
- Un evento malo nunca detiene el consumidor: queda guardado con su estado y
  motivo dentro del lote.
- Una falla de infraestructura (Kafka o PostgreSQL) revierte el lote completo
  y detiene el consumidor sin confirmar offsets; al reiniciarlo Kafka reentrega
  el lote y se procesa entero.
- Si el lote quedo en la base pero el offset no llego a Kafka, la reentrega se
  omite (UNIQUE de topic, particion y desplazamiento -> YA_PROCESADO).
"""
import logging
import time
from collections import Counter
from datetime import datetime, timezone

from src.streaming import almacenamiento
from src.streaming import contrato as c

log = logging.getLogger('consumidor')

YA_PROCESADO = 'YA_PROCESADO'  # solo para el resumen: reentrega de un mensaje ya confirmado
LARGO_MAXIMO_MOTIVO = 1000

# Vigilancia del broker. Con el broker caido, la libreria de Kafka reintenta en
# silencio y poll() solo devuelve "sin mensajes": el consumidor quedaba esperando
# para siempre y, en Docker Desktop para Mac, no retomaba al volver el broker
# (verificado con docker compose stop kafka). Por eso cada SONDEO_BROKER_S se
# piden los metadatos del topic; si Kafka no responde durante LIMITE_SIN_BROKER_S
# seguidos, el consumidor se detiene con un error claro. Al reiniciarlo retoma
# desde el ultimo offset confirmado.
SONDEO_BROKER_S = 10
LIMITE_SIN_BROKER_S = 30
ESPERA_SONDEO_S = 5

# Mensajes por transaccion. 200 a ~100 eventos/s es ~2 s en el peor caso; si
# llegan menos, el lote es lo que haya en ese momento (no se espera a llenarlo).
TAMANO_LOTE = 200


def procesar_lote(conexion, mensajes, ahora=lambda: datetime.now(timezone.utc)) -> list[str]:
    """Procesa un micro-lote en UNA transaccion. Devuelve el estado final de cada mensaje, en orden.

    La validacion es mensaje por mensaje (contrato y referencias); las
    escrituras se hacen en bloque (src/streaming/almacenamiento.py). Si la base
    falla, se revierte el lote entero y la excepcion sube: el consumidor se
    detiene sin confirmar offsets.
    """
    try:
        with conexion.cursor() as cursor:
            crudos = almacenamiento.guardar_crudos(cursor, mensajes)
            dimensiones = almacenamiento.leer_dimensiones(cursor)
            estados = {}      # posicion en el lote -> (estado, motivo)
            validos = []      # (posicion, id_crudo, evento, referencias)
            for posicion, mensaje in enumerate(mensajes):
                id_crudo, ya_procesado = crudos[(mensaje.topic(), mensaje.partition(), mensaje.offset())]
                if ya_procesado:
                    estados[posicion] = (YA_PROCESADO, None)
                    continue
                resultado = c.validar_evento(mensaje.value(), mensaje.key())
                if not resultado.es_valido:
                    estados[posicion] = (resultado.estado, resultado.motivo)
                    continue
                referencias, errores = almacenamiento.resolver_referencias(dimensiones, resultado.evento)
                if errores:
                    estados[posicion] = (c.INVALIDO_REFERENCIA, '; '.join(errores))
                    continue
                validos.append((posicion, id_crudo, resultado.evento, referencias))

            insertados = almacenamiento.guardar_hechos(
                cursor, [(id_crudo, evento, ref) for _, id_crudo, evento, ref in validos], ahora())
            almacenamiento.actualizar_metricas(
                cursor, [(evento, ref) for _, id_crudo, evento, ref in validos if id_crudo in insertados])
            for posicion, id_crudo, evento, _ in validos:
                if id_crudo in insertados:
                    estados[posicion] = (c.VALIDO, None)
                else:
                    estados[posicion] = (c.DUPLICADO, f'el hogar ({evento[c.DIRECTORIO]}, {evento[c.ORDEN]}) o el '
                                                      f'id_evento {evento[c.ID_EVENTO]} ya se habia recibido')

            por_marcar = []
            for posicion, mensaje in enumerate(mensajes):
                estado, motivo = estados[posicion]
                if estado == YA_PROCESADO:
                    log.info('[p%s o%s] reentrega de un mensaje ya procesado: se omite',
                             mensaje.partition(), mensaje.offset())
                    continue
                motivo = motivo[:LARGO_MAXIMO_MOTIVO] if motivo else None
                if estado != c.VALIDO:
                    log.warning('[p%s o%s] %s: %s', mensaje.partition(), mensaje.offset(), estado, motivo)
                por_marcar.append((crudos[(mensaje.topic(), mensaje.partition(), mensaje.offset())][0],
                                   estado, motivo))
            almacenamiento.marcar_estados(cursor, por_marcar)
        conexion.commit()
    except Exception:
        conexion.rollback()
        raise
    return [estados[posicion][0] for posicion in range(len(mensajes))]


def procesar_mensaje(conexion, mensaje, ahora=lambda: datetime.now(timezone.utc)) -> str:
    """Un solo mensaje en su propia transaccion (lote de uno)."""
    return procesar_lote(conexion, [mensaje], ahora)[0]


class Parada:
    """Pedido de parada (Ctrl+C o SIGTERM): se termina el mensaje en curso y se sale limpio."""

    def __init__(self):
        self.pedida = False

    def pedir(self, *_):
        self.pedida = True


def _broker_responde(consumidor, topic):
    """True si Kafka entrega los metadatos del topic a tiempo."""
    from confluent_kafka import KafkaException
    try:
        metadatos = consumidor.list_topics(topic, timeout=ESPERA_SONDEO_S)
    except KafkaException:
        return False
    encontrado = metadatos.topics.get(topic)
    return encontrado is not None and encontrado.error is None


def consumir(consumidor, conexion, topic, parada, tiempo_inactivo=None, conteos=None, errores_broker=None,
             reloj=time.monotonic, cada=500, tamano_lote=TAMANO_LOTE):
    """Ciclo principal: recibe un micro-lote, lo procesa en una transaccion y confirma sus offsets.

    Devuelve los conteos por estado.
    """
    from confluent_kafka import KafkaError, KafkaException

    conteos = Counter() if conteos is None else conteos
    asignadas = []

    def al_asignar(_consumidor, particiones):
        asignadas.extend(particiones)
        log.info('Particiones asignadas: %s', sorted(p.partition for p in particiones))

    consumidor.subscribe([topic], on_assign=al_asignar)
    ultima_actividad = None
    procesados = 0
    proximo_sondeo = reloj() + SONDEO_BROKER_S
    sin_broker_desde = None
    while not parada.pedida:
        recibidos = consumidor.consume(num_messages=tamano_lote, timeout=1.0)
        if errores_broker:
            raise RuntimeError(f'Se perdio la conexion con Kafka: {errores_broker[0]}')
        if reloj() >= proximo_sondeo:
            proximo_sondeo = reloj() + SONDEO_BROKER_S
            if _broker_responde(consumidor, topic):
                sin_broker_desde = None
            else:
                sin_broker_desde = sin_broker_desde or reloj()
                log.warning('Kafka no responde desde hace %.0f s', reloj() - sin_broker_desde)
                if reloj() - sin_broker_desde >= LIMITE_SIN_BROKER_S:
                    raise RuntimeError(f'Kafka no responde hace {LIMITE_SIN_BROKER_S} s o mas: se detiene el '
                                       'consumidor. Al volver a arrancarlo retoma desde el ultimo offset confirmado.')
        mensajes = []
        for mensaje in recibidos:
            error = mensaje.error()
            if error is None:
                mensajes.append(mensaje)
            elif error.code() == KafkaError._PARTITION_EOF:
                continue
            elif error.code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                raise RuntimeError(f"El topic '{topic}' no existe. Lo crea el servicio kafka-init o "
                                   'python streaming/reiniciar_demo.py.')
            else:
                raise KafkaException(error)
        if not mensajes:
            if not asignadas:
                # Sin particiones aun: Kafka puede tardar si el consumidor anterior murio
                # de golpe. El reloj de inactividad no corre todavia.
                continue
            ultima_actividad = ultima_actividad or reloj()
            if tiempo_inactivo is not None and reloj() - ultima_actividad >= tiempo_inactivo:
                log.info('Sin mensajes nuevos en %s s: se termina', tiempo_inactivo)
                break
            continue
        conteos.update(procesar_lote(conexion, mensajes))
        # Solo ahora, con el lote guardado en la base, se confirman sus offsets en Kafka.
        # Asincrono: no espera la respuesta del broker. Si el proceso cae antes de que
        # llegue, Kafka reentrega el lote y la base lo omite (YA_PROCESADO).
        consumidor.commit(asynchronous=True)
        ultima_actividad = reloj()
        anteriores, procesados = procesados, procesados + len(mensajes)
        if procesados // cada > anteriores // cada:
            log.info('%s mensajes atendidos: %s', procesados, dict(conteos))
    return conteos
