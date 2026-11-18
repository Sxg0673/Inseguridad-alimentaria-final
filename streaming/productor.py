"""Productor de Kafka: re-publica los hogares del DW como llegada simulada de encuestas.

SIMULACION: la ECV 2025 ya esta cargada en el Data Warehouse. Este productor
lee fact_seguridad_alimentaria_hogar (con su geografia y su severidad) y la
publica hogar por hogar en el topic encuestas-hogares, como si las encuestas
llegaran del trabajo de campo. Clave del mensaje = codigo del departamento.

Uso (con el DW cargado y Kafka arriba; ver README, seccion 10):
    python streaming/productor.py                          5.000 hogares a 50 por segundo (~100 s)
    python streaming/productor.py --eventos-por-segundo 200
    python streaming/productor.py --con-invalidos          intercala 6 eventos danados a proposito
    python streaming/productor.py --completo               los 87.060 hogares sin pausa (prueba de coherencia)

Codigo de salida: 0 si todo se entrego, 1 si fallo, 130 si se interrumpio con Ctrl+C.
"""
import argparse
import logging
import sys
import time

import psycopg2

from src.streaming import config, fuente
from src.streaming.publicacion import CASOS_INVALIDOS, Estadisticas, construir_plan, enviar

log = logging.getLogger('productor')

EVENTOS_POR_SEGUNDO = 50
LIMITE = 5000
SEMILLA = 42
ESPERA_TOPIC_S = 30


def _positivo(tipo):
    def convertir(texto):
        valor = tipo(texto)
        if not 0 < valor < float('inf'):
            raise argparse.ArgumentTypeError('debe ser mayor que 0')
        return valor
    return convertir


def leer_argumentos(argv=None):
    parser = argparse.ArgumentParser(
        description='Re-publica los hogares del DW en Kafka (llegada simulada de encuestas).')
    parser.add_argument('--eventos-por-segundo', type=_positivo(float), default=EVENTOS_POR_SEGUNDO,
                        help=f'ritmo de publicacion (por defecto {EVENTOS_POR_SEGUNDO})')
    parser.add_argument('--limite', type=_positivo(int), default=LIMITE,
                        help=f'cuantos hogares publicar (por defecto {LIMITE})')
    parser.add_argument('--completo', action='store_true',
                        help='todos los hogares y sin pausa (ignora --limite y --eventos-por-segundo)')
    parser.add_argument('--con-invalidos', action='store_true',
                        help=f'intercalar {len(CASOS_INVALIDOS) + 1} eventos danados a proposito')
    parser.add_argument('--semilla', type=int, default=SEMILLA,
                        help=f'semilla del orden de envio (por defecto {SEMILLA})')
    return parser.parse_args(argv)


def crear_productor(servidores):
    from confluent_kafka import Producer
    return Producer({
        'bootstrap.servers': servidores,
        'client.id': 'productor-encuestas',
        # Idempotencia: el broker confirma cada evento (acks=all) y descarta los
        # duplicados de un reintento interno, asi que no se pierde ni se repite ninguno.
        'enable.idempotence': True,
        'linger.ms': 5,
        'message.timeout.ms': 30000,
        'log_level': 2,
    })


def comprobar_topic(productor, topic):
    """Falla con un mensaje claro si Kafka no responde o el topic no existe (espera hasta 30 s)."""
    from confluent_kafka import KafkaException
    limite = time.monotonic() + ESPERA_TOPIC_S
    while True:
        try:
            metadatos = productor.list_topics(topic, timeout=10)
        except KafkaException as error:
            raise RuntimeError(f'No hay conexion con Kafka ({error}). Levantarlo con: '
                               'docker compose --profile streaming up -d warehouse kafka kafka-init') from error
        encontrado = metadatos.topics.get(topic)
        if encontrado is not None and encontrado.error is None:
            if len(encontrado.partitions) != config.PARTICIONES_TOPIC:
                log.warning('El topic tiene %s particiones y se esperaban %s', len(encontrado.partitions),
                            config.PARTICIONES_TOPIC)
            return
        if time.monotonic() >= limite:
            raise RuntimeError(f"El topic '{topic}' no existe. Lo crea el servicio kafka-init o "
                               'python streaming/reiniciar_demo.py.')
        time.sleep(1)


def avisar_si_hay_datos(conexion):
    """Si el streaming ya tiene hogares, reenviar los mismos producira DUPLICADO (no bloquea)."""
    with conexion.cursor() as cursor:
        cursor.execute('SELECT count(*) FROM fact_evento_encuesta')
        recibidos = cursor.fetchone()[0]
    conexion.rollback()
    if recibidos:
        log.warning('El streaming ya tiene %s hogares recibidos: si se reenvian, quedaran como DUPLICADO. '
                    'Para empezar de cero: python streaming/reiniciar_demo.py', f'{recibidos:,}')


def main(argv=None, crear=crear_productor):
    args = leer_argumentos(argv)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)-7s %(name)s: %(message)s')
    config.cargar_entorno()
    topic = config.topic_encuestas()

    try:
        conexion = psycopg2.connect(config.url_warehouse(), connect_timeout=10)
    except psycopg2.OperationalError as error:
        log.error('No hay conexion con el warehouse (%s). Revisar ETL_DATABASE_URL en .env y que el '
                  'servicio warehouse este arriba.', str(error).strip())
        return 1
    try:
        hogares = fuente.leer_hogares(conexion)
        avisar_si_hay_datos(conexion)
    except psycopg2.Error as error:
        log.error('No se pudo leer el DW (%s). Correr primero el DAG inseguridad_alimentaria_etl.', str(error).strip())
        return 1
    finally:
        conexion.close()
    if not hogares:
        log.error('El hecho de hogares esta vacio. Correr primero el DAG inseguridad_alimentaria_etl.')
        return 1

    limite = None if args.completo else args.limite
    ritmo = None if args.completo else args.eventos_por_segundo
    seleccion = fuente.ordenar_para_envio(hogares, args.semilla, limite)
    plan = construir_plan(seleccion, args.con_invalidos)
    estadisticas = Estadisticas(total=len(plan))
    log.info('SIMULACION: %s de %s hogares del DW en %s, %s', f'{len(seleccion):,}', f'{len(hogares):,}', topic,
             'sin pausa' if ritmo is None else f'a {ritmo:g} eventos/s')

    productor = crear(config.servidores_kafka())
    inicio = time.monotonic()
    codigo = 0
    try:
        comprobar_topic(productor, topic)
        enviar(productor, topic, plan, ritmo, estadisticas)
    except RuntimeError as error:
        log.error('%s', error)
        codigo = 1
    except KeyboardInterrupt:
        productor.flush(10)  # se entrega lo que ya estaba en camino
        log.warning('Interrumpido con Ctrl+C')
        codigo = 130
    duracion = time.monotonic() - inicio
    log.info('%s: %s de %s eventos entregados en %.1f s (%.0f eventos/s)',
             'Listo' if codigo == 0 else 'Envio incompleto', f'{estadisticas.entregados:,}',
             f'{estadisticas.total:,}', duracion, estadisticas.entregados / duracion if duracion else 0)
    return codigo


if __name__ == '__main__':
    sys.exit(main())
