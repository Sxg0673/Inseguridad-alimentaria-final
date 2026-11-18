"""Precarga la demo: el operativo de campo ya va avanzado cuando se abre el tablero.

SIMULACION: copia un porcentaje de los hogares del DW directamente a las tablas
del streaming (origen 'precarga'), en el mismo orden que usa el productor. Despues
el productor publica por Kafka solo los que faltan:

    python streaming/reiniciar_demo.py --si
    python streaming/precargar.py --porcentaje 90        ~78.000 hogares en segundos
    python streaming/consumidor.py                       (otra terminal)
    python streaming/productor.py --pendientes           ~8.700 hogares por Kafka

Al terminar, la prevalencia acumulada coincide con la del DW (21,10 %).
Detalle en src/streaming/precarga.py.
"""
import argparse
import logging
import sys

import psycopg2

from src.streaming import config, fuente
from src.streaming.precarga import StreamingNoVacio, precargar

log = logging.getLogger('precarga')

PORCENTAJE = 90
SEMILLA = 42  # la misma del productor: la precarga toma los primeros y --pendientes el resto


def _porcentaje(texto):
    valor = float(texto)
    if not 0 < valor < 100:
        raise argparse.ArgumentTypeError('debe estar entre 0 y 100 (sin incluirlos)')
    return valor


def main(argv=None):
    parser = argparse.ArgumentParser(description='Precarga un porcentaje de hogares del DW en el streaming.')
    parser.add_argument('--porcentaje', type=_porcentaje, default=PORCENTAJE,
                        help=f'porcentaje de hogares ya recibidos al iniciar la demo (por defecto {PORCENTAJE})')
    parser.add_argument('--semilla', type=int, default=SEMILLA,
                        help=f'semilla del orden (por defecto {SEMILLA}, la misma del productor)')
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)-7s %(name)s: %(message)s')
    config.cargar_entorno()
    try:
        conexion = psycopg2.connect(config.url_warehouse(), connect_timeout=10)
    except psycopg2.OperationalError as error:
        log.error('No hay conexion con el warehouse (%s). Revisar ETL_DATABASE_URL en .env.', str(error).strip())
        return 1
    try:
        hogares = fuente.leer_hogares(conexion)
        if not hogares:
            log.error('El hecho de hogares esta vacio. Correr primero el DAG inseguridad_alimentaria_etl.')
            return 1
        cantidad = round(len(hogares) * args.porcentaje / 100)
        seleccion = fuente.ordenar_para_envio(hogares, args.semilla, cantidad)
        precargados = precargar(conexion, seleccion)
    except StreamingNoVacio as error:
        log.error('%s', error)
        return 1
    finally:
        conexion.close()
    log.info('SIMULACION: %s de %s hogares (%.0f %%) quedan como ya recibidos. Faltan %s: '
             'publicarlos con python streaming/productor.py --pendientes', f'{precargados:,}', f'{len(hogares):,}',
             args.porcentaje, f'{len(hogares) - precargados:,}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
