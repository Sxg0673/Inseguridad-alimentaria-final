"""Exportacion de la evidencia de una corrida a docs/evidencia/."""
import importlib.util
import json
from pathlib import Path

import pytest

from src import evidencia
from src.comun import lotes
from src.comun.errores import ErrorDeterminista

RAIZ = Path(__file__).resolve().parents[1]


def _resultado(regla, exito=True, severidad='critical', **extra):
    return {'regla': regla, 'descripcion': f'regla {regla}', 'severidad': severidad, 'tabla': 'tabla_x',
            'expectativa': 'expect_x', 'parametros': {}, 'exito': exito, 'observado': None,
            'inesperados': 0, 'ejemplos': [], 'excepcion': None, **extra}


def _compuerta(lote, numero, estado, resultados, criticas=(), advertencias=(), metricas=None):
    ruta = Path(lote) / 'calidad' / f'compuerta_{numero}.json'
    lotes.escribir_json(ruta, {
        'compuerta': numero, 'nombre': f'Compuerta {numero}', 'fecha_utc': lotes.ahora_utc(),
        'estado': estado, 'reglas_evaluadas': sorted({r['regla'] for r in resultados}),
        'criticas_fallidas': list(criticas), 'advertencias': list(advertencias),
        'metricas': metricas or {}, 'resultados': resultados,
    })
    lotes.registrar_archivo(lote, 'calidad', ruta, estado=estado)


def _carga(lote):
    ruta = Path(lote) / 'carga' / 'carga.json'
    lotes.escribir_json(ruta, {
        'fecha_utc': lotes.ahora_utc(), 'estado_compuerta_3': 'aprobada_con_advertencias',
        'estrategia': 'TRUNCATE + INSERT en una transaccion',
        'filas_por_tabla': {'dim_geografia': 65, 'fact_seguridad_alimentaria_hogar': 87060},
        'tablas_streaming_reiniciadas': ['stg_evento_encuesta'], 'total_filas': 87125,
    })
    lotes.registrar_archivo(lote, 'carga', ruta)


def lote_exitoso(run_id):
    lote = lotes.crear_lote(run_id)
    _compuerta(lote, 1, 'aprobada', [_resultado('C1')])
    _compuerta(lote, 2, 'aprobada', [_resultado('C7')])
    _compuerta(lote, 3, 'aprobada_con_advertencias',
               [_resultado('C12'), _resultado('A3', exito=False, severidad='warning', observado=0.3)],
               advertencias=['A3'], metricas={'prevalencia_nacional_hecho': 21.0956})
    _carga(lote)
    return lote


def lote_fallido(run_id):
    lote = lotes.crear_lote(run_id)
    _compuerta(lote, 1, 'aprobada', [_resultado('C1')])
    _compuerta(lote, 2, 'rechazada',
               [_resultado('C7', exito=False, inesperados=4, ejemplos=['9', '9', '7', '8'])],
               criticas=['C7'])
    return lote


def _fijar_fecha(lote, fecha):
    """Fecha de creacion controlada para que el orden no dependa del reloj."""
    datos = lotes.leer_json(Path(lote) / 'lote.json')
    datos['creado_utc'] = fecha
    lotes.escribir_json(Path(lote) / 'lote.json', datos)


def test_exporta_el_ultimo_lote_cargado(entorno, tmp_path):
    viejo = lote_exitoso('run_viejo')
    nuevo = lote_exitoso('run_nuevo')
    fallido = lote_fallido('run_fallido')
    _fijar_fecha(viejo, '2026-01-01T00:00:00+00:00')
    _fijar_fecha(nuevo, '2026-01-02T00:00:00+00:00')
    _fijar_fecha(fallido, '2026-01-03T00:00:00+00:00')   # el mas reciente, pero sin carga

    carpeta = evidencia.exportar(destino=tmp_path / 'evidencia')

    assert carpeta == tmp_path / 'evidencia' / 'corrida'
    assert sorted(p.name for p in carpeta.iterdir()) == [
        'carga.json', 'compuerta_1.json', 'compuerta_2.json', 'compuerta_3.json', 'lote.json', 'resumen.md']
    assert json.loads((carpeta / 'lote.json').read_text(encoding='utf-8'))['run_id'] == 'run_nuevo'
    for nombre in ('compuerta_1.json', 'compuerta_2.json', 'compuerta_3.json'):
        assert (carpeta / nombre).read_bytes() == (Path(nuevo) / 'calidad' / nombre).read_bytes()
    resumen = (carpeta / 'resumen.md').read_text(encoding='utf-8')
    assert 'cargada en el warehouse' in resumen
    assert '| **Total** | **87.125** |' in resumen
    assert '**A3** (advertencia)' in resumen
    assert '| prevalencia_nacional_hecho | 21,0956 |' in resumen
    assert '| fact_seguridad_alimentaria_hogar | 87.060 |' in resumen


def test_exporta_una_falla_controlada_con_las_reglas_criticas(entorno, tmp_path):
    fallido = lote_fallido('corrida_falla')
    carpeta = evidencia.exportar(Path(fallido).name, 'falla_controlada', tmp_path)

    assert not (carpeta / 'carga.json').exists()
    resumen = (carpeta / 'resumen.md').read_text(encoding='utf-8')
    assert 'detenida antes de la carga' in resumen
    assert '| 2 | Compuerta 2 | rechazada | 1 | C7 | - |' in resumen
    assert '1 compuerta no se ejecutó' in resumen
    assert '**C7** (crítica)' in resumen and '4 valores inesperados' in resumen
    assert '(ejemplos: `9`, `9`, `7`)' in resumen          # solo los primeros ejemplos


def test_reporte_alterado_no_se_exporta(entorno, tmp_path):
    lote = lote_exitoso('run_1')
    (Path(lote) / 'calidad' / 'compuerta_2.json').write_text('{"alterado": true}', encoding='utf-8')
    with pytest.raises(ErrorDeterminista, match='cambio'):
        evidencia.exportar(destino=tmp_path)
    assert not (tmp_path / 'corrida').exists()


def test_reexportar_reemplaza_la_carpeta(entorno, tmp_path):
    lote_exitoso('run_1')
    carpeta = evidencia.exportar(destino=tmp_path)
    (carpeta / 'sobrante.txt').write_text('x', encoding='utf-8')
    evidencia.exportar(destino=tmp_path)
    assert not (carpeta / 'sobrante.txt').exists()


def test_sin_lotes_cargados_o_lote_inexistente(entorno, tmp_path):
    with pytest.raises(ErrorDeterminista, match='Correr primero el DAG'):
        evidencia.exportar(destino=tmp_path)
    lote_fallido('run_1')
    with pytest.raises(ErrorDeterminista, match='ningun lote cargado'):
        evidencia.exportar(destino=tmp_path)
    with pytest.raises(ErrorDeterminista, match='No existe el lote'):
        evidencia.exportar('no_existe', destino=tmp_path)


def test_script_lista_y_exporta(entorno, tmp_path, monkeypatch, capsys):
    spec = importlib.util.spec_from_file_location('script_evidencia', RAIZ / 'scripts' / 'exportar_evidencia.py')
    script = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(script)
    monkeypatch.setattr(evidencia, 'DESTINO_POR_DEFECTO', tmp_path / 'evidencia')

    assert script.main(['--listar']) == 0
    assert 'No hay lotes' in capsys.readouterr().out
    assert script.main([]) == 1                              # nada que exportar

    lote = lote_exitoso('run_1')
    assert script.main(['--listar']) == 0
    salida = capsys.readouterr().out
    assert Path(lote).name in salida and 'compuerta_3=aprobada_con_advertencias' in salida
    assert script.main(['--nombre', 'prueba']) == 0
    assert (tmp_path / 'evidencia' / 'prueba' / 'resumen.md').exists()
