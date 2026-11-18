"""Consumidor de Kafka: valida cada encuesta y actualiza el modelo y las metricas del streaming.

Por cada mensaje del topic encuestas-hogares (grupo consumidor-encuestas):
staging -> contrato -> referencias en las dimensiones -> hecho -> snapshot.
Procesa micro-lotes (hasta 200 mensajes) en una transaccion y despues confirma
sus offsets (src/streaming/procesamiento.py).

Uso (con el DW cargado y Kafka arriba):
    python streaming/consumidor.py                        escucha hasta Ctrl+C
    python streaming/consumidor.py --tiempo-inactivo 15   termina tras 15 s sin mensajes

Codigo de salida: 0 si termino bien (Ctrl+C incluido), 1 si se detuvo por una falla.
"""
import argparse
import logging
import signal
import sys
import time
from collections import Counter

import psycopg2

from src.streaming import config
from src.streaming.procesamiento import Parada, consumir

log = logging.getLogger('consumidor')


def _segundos(texto):
    valor = float(texto)
    if not 0 < valor < float('inf'):
        raise argparse.ArgumentTypeError('debe ser un numero de segundos mayor que 0')
    return valor


def leer_argumentos(argv=None):
    parser = argparse.ArgumentParser(description='Consume encuestas de Kafka y actualiza el streaming en el DW.')
    parser.add_argument('--tiempo-inactivo', type=_segundos, default=None,
                        help='terminar tras N segundos sin mensajes (por defecto escucha hasta Ctrl+C)')
    return parser.parse_args(argv)


def crear_consumidor(servidores, grupo, errores_broker):
    from confluent_kafka import Consumer, KafkaError

    def al_error(error):
        # La libreria reintenta sola los cortes breves; se detiene si no queda ningun broker
        if error.code() == KafkaError._ALL_BROKERS_DOWN or error.fatal():
            errores_broker.append(error.str())

    return Consumer({
        'bootstrap.servers': servidores,
        'group.id': grupo,
        'client.id': 'consumidor-encuestas',
        # El offset se confirma a mano, despues de guardar en la base: si el
        # consumidor cae a mitad de un mensaje, Kafka lo vuelve a entregar.
        'enable.auto.commit': False,
        # Un grupo nuevo empieza desde el primer mensaje del topic.
        'auto.offset.reset': 'earliest',
        # Si el consumidor muere de golpe, Kafka lo da por perdido a los 10 s
        # (por defecto son 45) y entrega sus particiones al siguiente.
        'session.timeout.ms': 10000,
        'log_level': 2,
        'error_cb': al_error,
    })


def main(argv=None, crear=crear_consumidor, instalar_senales=True):
    args = leer_argumentos(argv)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)-7s %(name)s: %(message)s')
    config.cargar_entorno()
    topic, grupo = config.topic_encuestas(), config.grupo_consumidor()
    try:
        conexion = psycopg2.connect(config.url_warehouse(), connect_timeout=10)
    except psycopg2.OperationalError as error:
        log.error('No hay conexion con el warehouse (%s). Revisar ETL_DATABASE_URL en .env.', str(error).strip())
        return 1

    parada = Parada()
    if instalar_senales:
        signal.signal(signal.SIGINT, parada.pedir)
        signal.signal(signal.SIGTERM, parada.pedir)
    errores_broker = []
    consumidor = crear(config.servidores_kafka(), grupo, errores_broker)
    conteos = Counter()
    inicio = time.monotonic()
    codigo = 0
    log.info("Escuchando '%s' en %s como grupo '%s'. Ctrl+C para detener.", topic, config.servidores_kafka(), grupo)
    try:
        consumir(consumidor, conexion, topic, parada, args.tiempo_inactivo, conteos, errores_broker)
    except psycopg2.Error as error:
        log.error('Falla de PostgreSQL: %s. El mensaje en curso no se confirmo: se retomara al reiniciar.',
                  str(error).strip())
        codigo = 1
    except Exception as error:  # noqa: BLE001 - Kafka caido u otra falla de infraestructura
        log.error('%s', error)
        codigo = 1
    finally:
        try:
            consumidor.close()  # sale del grupo ordenadamente
        finally:
            conexion.close()
    if parada.pedida:
        log.info('Detenido a pedido (Ctrl+C)')
    resumen = ', '.join(f'{n} {estado}' for estado, n in sorted(conteos.items())) or 'ninguno'
    log.info('Mensajes atendidos en %.1f s: %s', time.monotonic() - inicio, resumen)
    return codigo


if __name__ == '__main__':
    sys.exit(main())
