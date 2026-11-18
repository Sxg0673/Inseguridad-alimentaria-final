"""
Logica del consumidor: procesar un mensaje y ciclo de consumo.

Por cada mensaje del topic, en UNA transaccion de la base:
  1. Guarda el mensaje tal como llego en stg_evento_encuesta (antes de validar).
  2. Valida esquema y valores con el contrato -> INVALIDO_ESQUEMA / INVALIDO_VALORES.
  3. Resuelve las FK por clave natural en las dimensiones -> INVALIDO_REFERENCIA.
  4. Inserta en fact_evento_encuesta; si el hogar ya estaba -> DUPLICADO.
  5. Actualiza fact_metrica_departamento_stream y marca el crudo como VALIDO.
Despues del commit de la base, confirma el offset en Kafka (asincrono).

Un evento malo nunca detiene el consumidor: queda guardado con su estado y
motivo. Una falla de infraestructura (Kafka o PostgreSQL) si lo detiene; al
reiniciarlo, Kafka reentrega desde el ultimo offset confirmado y la base
(UNIQUE de topic, particion y desplazamiento) evita duplicados.
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


def procesar_mensaje(conexion, mensaje, ahora=lambda: datetime.now(timezone.utc)) -> str:
    """Procesa un mensaje de punta a punta en una transaccion. Devuelve el estado final.

    Las excepciones de la base se dejan pasar: son fallas de infraestructura,
    deben detener el consumidor y el offset no se confirma.
    """
    donde = f'[p{mensaje.partition()} o{mensaje.offset()}]'
    try:
        with conexion.cursor() as cursor:
            id_crudo, ya_procesado = almacenamiento.guardar_crudo(
                cursor, mensaje.topic(), mensaje.partition(), mensaje.offset(), mensaje.key(), mensaje.value())
            if ya_procesado:
                conexion.commit()
                log.info('%s reentrega de un mensaje ya procesado: se omite', donde)
                return YA_PROCESADO

            resultado = c.validar_evento(mensaje.value(), mensaje.key())
            if not resultado.es_valido:
                return _cerrar_invalido(conexion, cursor, id_crudo, resultado.estado, resultado.motivo, donde)

            evento = resultado.evento
            referencias, errores = almacenamiento.resolver_referencias(cursor, evento)
            if errores:
                return _cerrar_invalido(conexion, cursor, id_crudo, c.INVALIDO_REFERENCIA, '; '.join(errores), donde)

            if not almacenamiento.guardar_evento_valido(cursor, id_crudo, evento, referencias, ahora()):
                motivo = (f'el hogar ({evento[c.DIRECTORIO]}, {evento[c.ORDEN]}) o el id_evento '
                          f'{evento[c.ID_EVENTO]} ya se habia recibido')
                return _cerrar_invalido(conexion, cursor, id_crudo, c.DUPLICADO, motivo, donde)

            almacenamiento.marcar(cursor, id_crudo, c.VALIDO)
        conexion.commit()
    except Exception:
        conexion.rollback()
        raise
    log.debug('%s VALIDO %s-%s depto %s', donde, evento[c.DIRECTORIO], evento[c.ORDEN], evento[c.DEPARTAMENTO])
    return c.VALIDO


def _cerrar_invalido(conexion, cursor, id_crudo, estado, motivo, donde):
    motivo = (motivo or '')[:LARGO_MAXIMO_MOTIVO]
    almacenamiento.marcar(cursor, id_crudo, estado, motivo)
    conexion.commit()
    log.warning('%s %s: %s', donde, estado, motivo)
    return estado


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
             reloj=time.monotonic, cada=500):
    """Ciclo principal: recibe, procesa y confirma mensaje por mensaje. Devuelve los conteos por estado."""
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
        mensaje = consumidor.poll(1.0)
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
        if mensaje is None:
            if not asignadas:
                # Sin particiones aun: Kafka puede tardar si el consumidor anterior murio
                # de golpe. El reloj de inactividad no corre todavia.
                continue
            ultima_actividad = ultima_actividad or reloj()
            if tiempo_inactivo is not None and reloj() - ultima_actividad >= tiempo_inactivo:
                log.info('Sin mensajes nuevos en %s s: se termina', tiempo_inactivo)
                break
            continue
        error = mensaje.error()
        if error is not None:
            if error.code() == KafkaError._PARTITION_EOF:
                continue
            if error.code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                raise RuntimeError(f"El topic '{topic}' no existe. Lo crea el servicio kafka-init o "
                                   'python streaming/reiniciar_demo.py.')
            raise KafkaException(error)
        conteos[procesar_mensaje(conexion, mensaje)] += 1
        # Solo ahora, con todo guardado en la base, se confirma el offset en Kafka.
        # Asincrono: no espera la respuesta del broker en cada mensaje (en el Mac de
        # pruebas, la confirmacion sincrona limitaba el consumidor a ~70 eventos/s).
        # Si el proceso cae antes de que llegue la confirmacion, Kafka reentrega esos
        # mensajes y la base los omite (UNIQUE de topic, particion y desplazamiento).
        consumidor.commit(message=mensaje, asynchronous=True)
        ultima_actividad = reloj()
        procesados += 1
        if procesados % cada == 0:
            log.info('%s mensajes atendidos: %s', procesados, dict(conteos))
    return conteos
