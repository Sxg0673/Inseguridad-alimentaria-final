"""Deja la demo del streaming en cero: sin eventos, sin metricas y con el topic vacio.

Borra SOLO los datos del streaming:
  1. Vacia stg_evento_encuesta, fact_evento_encuesta y fact_metrica_departamento_stream
     (el modelo batch y las dimensiones no se tocan).
  2. Borra el consumer group (sus offsets guardados).
  3. Borra y vuelve a crear el topic (3 particiones).

Las tres cosas van juntas a proposito: si se recreara el topic sin vaciar las
tablas, los offsets volverian a empezar en 0 y chocarian con los ya guardados.

Uso (con el consumidor DETENIDO, el warehouse y Kafka arriba):
    python streaming/reiniciar_demo.py        pide confirmacion
    python streaming/reiniciar_demo.py --si   sin preguntar
"""
import argparse
import sys
import time

import psycopg2

from src.carga import TABLAS_STREAMING
from src.streaming import config

ESPERA_S = 30


def miembros_activos(admin, grupo):
    """Cuantos consumidores del grupo estan conectados (0 si el grupo no existe)."""
    from confluent_kafka import KafkaException
    try:
        descripcion = admin.describe_consumer_groups([grupo], request_timeout=ESPERA_S)[grupo].result()
    except KafkaException:
        return 0
    return len(descripcion.members)


def recrear_topic(admin, topic, particiones):
    from confluent_kafka import KafkaError, KafkaException
    from confluent_kafka.admin import NewTopic
    try:
        admin.delete_topics([topic], operation_timeout=ESPERA_S)[topic].result()
    except KafkaException as error:
        if error.args[0].code() != KafkaError.UNKNOWN_TOPIC_OR_PART:
            raise
    limite = time.monotonic() + ESPERA_S
    while True:  # el borrado es asincrono: crear puede fallar un instante con "ya existe"
        try:
            admin.create_topics([NewTopic(topic, num_partitions=particiones, replication_factor=1)],
                                operation_timeout=ESPERA_S)[topic].result()
            return
        except KafkaException as error:
            if error.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS or time.monotonic() > limite:
                raise
            time.sleep(0.5)


def main(argv=None):
    parser = argparse.ArgumentParser(description='Reinicia los datos de la demo del streaming.')
    parser.add_argument('--si', action='store_true', help='no pedir confirmacion')
    args = parser.parse_args(argv)
    config.cargar_entorno()
    from confluent_kafka import KafkaException
    from confluent_kafka.admin import AdminClient

    topic, grupo = config.topic_encuestas(), config.grupo_consumidor()
    admin = AdminClient({'bootstrap.servers': config.servidores_kafka(), 'log_level': 2})
    try:
        activos = miembros_activos(admin, grupo)
        conexion = psycopg2.connect(config.url_warehouse(), connect_timeout=10)
    except (KafkaException, psycopg2.OperationalError) as error:
        print(f'No hay conexion con Kafka o con el warehouse: {error}')
        return 1
    if activos:
        print(f"El grupo '{grupo}' tiene {activos} consumidor(es) activo(s). Detenerlo(s) con Ctrl+C y repetir.")
        conexion.close()
        return 1
    pregunta = f"Se borraran los datos del streaming y el topic '{topic}'. ¿Continuar? [s/N] "
    if not args.si and input(pregunta).strip().lower() != 's':
        print('Cancelado.')
        conexion.close()
        return 1

    with conexion, conexion.cursor() as cursor:
        cursor.execute(f"TRUNCATE TABLE {', '.join(TABLAS_STREAMING)} RESTART IDENTITY")
    conexion.close()
    print(f"Tablas vaciadas: {', '.join(TABLAS_STREAMING)}")
    try:
        admin.delete_consumer_groups([grupo], request_timeout=ESPERA_S)[grupo].result()
        print(f"Consumer group '{grupo}' borrado")
    except KafkaException:
        print(f"El consumer group '{grupo}' no existia")
    recrear_topic(admin, topic, config.PARTICIONES_TOPIC)
    print(f"Topic '{topic}' recreado con {config.PARTICIONES_TOPIC} particiones. Demo en cero.")
    return 0


if __name__ == '__main__':
    sys.exit(main())
