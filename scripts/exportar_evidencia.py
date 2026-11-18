"""Copia a docs/evidencia/ los reportes de calidad y carga de una corrida del DAG.

data/batches/ no se versiona; esta copia si. Uso habitual, despues de una
corrida exitosa:

    python scripts/exportar_evidencia.py                     ultimo lote cargado -> docs/evidencia/corrida/
    python scripts/exportar_evidencia.py --listar            lotes disponibles y su estado
    python scripts/exportar_evidencia.py --lote <id> --nombre falla_controlada

Detalle en src/evidencia.py.
"""
import argparse
import logging
import sys

from src import evidencia
from src.comun.errores import ErrorDeterminista

log = logging.getLogger('evidencia')


def _listar():
    disponibles = evidencia.listar_lotes()
    if not disponibles:
        print('No hay lotes en data/batches/. Correr primero el DAG.')
        return
    print(f"{'lote':<26}{'creado (UTC)':<28}{'cargado':<9}run_id / compuertas")
    for info in disponibles:
        estados = ', '.join(f"{nombre.removesuffix('.json')}={estado}"
                            for nombre, estado in info['compuertas'].items()) or 'sin compuertas'
        print(f"{info['id']:<26}{info['creado_utc']:<28}{'si' if info['cargado'] else 'no':<9}{info['run_id']}")
        print(f"{'':<63}{estados}")


def main(argv=None):
    parser = argparse.ArgumentParser(description='Exporta la evidencia de una corrida a docs/evidencia/.')
    parser.add_argument('--lote', help='id del lote (por defecto, el mas reciente que llego a la carga)')
    parser.add_argument('--nombre', default='corrida',
                        help='subcarpeta de docs/evidencia/ donde se copia (por defecto corrida)')
    parser.add_argument('--listar', action='store_true', help='solo lista los lotes disponibles')
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)-7s %(name)s: %(message)s')
    if args.listar:
        _listar()
        return 0
    try:
        carpeta = evidencia.exportar(args.lote, args.nombre)
    except ErrorDeterminista as error:
        log.error('%s', error)
        return 1
    log.info('Evidencia exportada en %s (%s archivos)', carpeta, len(list(carpeta.iterdir())))
    return 0


if __name__ == '__main__':
    sys.exit(main())
