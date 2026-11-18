"""
Configuracion del streaming: Kafka y conexion al warehouse desde el venv.

Sigue el patron de src/comun/config.py: funciones que leen variables de
entorno con un valor por defecto, para que un cambio de variable (por
ejemplo en pruebas) se refleje sin reimportar el modulo.

Los scripts de streaming/ corren en el venv del equipo, fuera de Docker.
Antes de leer la configuracion llaman a cargar_entorno(), que lee el .env
de la raiz. Las variables ya definidas en el sistema tienen prioridad.

Variables:
- ETL_DATABASE_URL          conexion al warehouse. Desde el venv:
                            postgresql://warehouse:<clave>@127.0.0.1:5434/warehouse
- KAFKA_BOOTSTRAP_SERVERS   direccion de Kafka (por defecto 127.0.0.1:9092)
- KAFKA_TOPIC               topic de las encuestas (por defecto encuestas-hogares)
- KAFKA_GRUPO_CONSUMIDOR    consumer group (por defecto consumidor-encuestas)
"""
import os

from src.comun.config import RAIZ, url_warehouse  # noqa: F401  (se reexporta para los scripts)

# Valores fijados en el diseno (README, seccion 8). El topic lo crea el
# servicio kafka-init del docker-compose con estas mismas particiones.
PARTICIONES_TOPIC = 3
VERSION_CONTRATO = 1


def cargar_entorno():
    """Lee el .env de la raiz sin pisar variables ya definidas en el sistema."""
    from dotenv import load_dotenv
    load_dotenv(RAIZ / '.env', override=False)


def servidores_kafka():
    return os.getenv('KAFKA_BOOTSTRAP_SERVERS') or '127.0.0.1:9092'


def topic_encuestas():
    return os.getenv('KAFKA_TOPIC') or 'encuestas-hogares'


def grupo_consumidor():
    return os.getenv('KAFKA_GRUPO_CONSUMIDOR') or 'consumidor-encuestas'
