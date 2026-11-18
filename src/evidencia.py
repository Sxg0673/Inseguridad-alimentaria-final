"""
Evidencia de una corrida: copia versionable de los reportes de calidad y carga.

Los lotes viven en data/batches/, que no se versiona (pesan cientos de MB por
los CSV congelados y los Parquet). Este modulo copia solo los reportes pequenos
de un lote a docs/evidencia/<nombre>/ para que queden en el repositorio:

    docs/evidencia/<nombre>/
        lote.json           run_id y fecha de creacion del lote
        compuerta_1.json    resultado de cada compuerta de Great Expectations
        compuerta_2.json
        compuerta_3.json
        carga.json          filas cargadas por tabla (solo si hubo carga)
        resumen.md          resumen legible de todo lo anterior

Antes de copiar verifica cada reporte contra el sha256 registrado en el
manifiesto del lote: si alguno cambio despues de generarse, no se exporta.

Un lote que se detuvo en una compuerta (por ejemplo la falla controlada de C7)
no tiene carga.json; se exporta igual y el resumen muestra las reglas criticas
que fallaron.
"""
import shutil
from pathlib import Path

from src.comun import lotes
from src.comun.config import RAIZ, dir_lotes
from src.comun.errores import ErrorDeterminista

DESTINO_POR_DEFECTO = RAIZ / 'docs' / 'evidencia'
EJEMPLOS_POR_REGLA = 3  # ejemplos de valores inesperados que se muestran en el resumen


def _info_lote(carpeta):
    """Datos basicos de un lote para listarlo y elegir el mas reciente."""
    datos = lotes.leer_json(carpeta / 'lote.json')
    compuertas = lotes.leer_manifiesto(carpeta, 'calidad')['archivos']
    estados = {nombre: info.get('estado') for nombre, info in sorted(compuertas.items())}
    return {
        'id': carpeta.name,
        'ruta': carpeta,
        'run_id': datos.get('run_id'),
        'creado_utc': datos.get('creado_utc', ''),
        'compuertas': estados,
        'cargado': (carpeta / 'carga' / 'carga.json').exists(),
    }


def listar_lotes():
    """Lotes con lote.json, del mas reciente al mas antiguo."""
    raiz = dir_lotes()
    if not raiz.is_dir():
        return []
    encontrados = [_info_lote(c) for c in raiz.iterdir() if (c / 'lote.json').is_file()]
    return sorted(encontrados, key=lambda info: info['creado_utc'], reverse=True)


def elegir_lote(id_lote=None):
    """El lote pedido o, si no se indica, el mas reciente que llego a la carga."""
    disponibles = listar_lotes()
    if id_lote:
        for info in disponibles:
            if info['id'] == id_lote:
                return info
        raise ErrorDeterminista(f'No existe el lote {id_lote} en {dir_lotes()}.')
    for info in disponibles:
        if info['cargado']:
            return info
    raise ErrorDeterminista(f'No hay ningun lote cargado en {dir_lotes()}. Correr primero el DAG.')


def _reportes(info):
    """(fuente, ruta relativa en la fuente) de cada reporte a exportar, ya verificado."""
    lote = info['ruta']
    reportes = [('calidad', nombre) for nombre in sorted(info['compuertas'])]
    if info['cargado']:
        reportes.append(('carga', 'carga.json'))
    if not reportes:
        raise ErrorDeterminista(f'El lote {info["id"]} no tiene reportes de calidad.')
    # verificar_archivo lanza ErrorDeterminista si falta o cambio despues de registrarse
    return [(fuente, relativa, lotes.verificar_archivo(lote, fuente, relativa))
            for fuente, relativa in reportes]


def _numero(valor, decimales=None):
    """Numero con formato colombiano: punto de miles y coma decimal."""
    texto = f'{valor:,.{decimales}f}' if decimales is not None else f'{valor:,}'
    return texto.replace(',', '_').replace('.', ',').replace('_', '.')


def _fila_compuerta(reporte):
    criticas = ', '.join(reporte['criticas_fallidas']) or '-'
    advertencias = ', '.join(reporte['advertencias']) or '-'
    return (f"| {reporte['compuerta']} | {reporte['nombre']} | {reporte['estado']} | "
            f"{len(reporte['reglas_evaluadas'])} | {criticas} | {advertencias} |")


def _detalle_fallas(reportes):
    """Lineas con cada expectativa que fallo, critica o advertencia."""
    lineas = []
    for reporte in reportes:
        for resultado in reporte['resultados']:
            if resultado['exito']:
                continue
            tipo = 'crítica' if resultado['severidad'] == 'critical' else 'advertencia'
            ejemplos = resultado.get('ejemplos') or []
            texto = (f"- Compuerta {reporte['compuerta']}, **{resultado['regla']}** ({tipo}): "
                     f"{resultado['descripcion']} — tabla `{resultado['tabla']}`")
            inesperados = resultado.get('inesperados') or 0
            if inesperados:
                texto += f", {_numero(inesperados)} {'valor inesperado' if inesperados == 1 else 'valores inesperados'}"
            if resultado.get('observado') is not None:
                texto += f", observado `{resultado['observado']}`"
            if ejemplos:
                muestra = ', '.join(f'`{e}`' for e in ejemplos[:EJEMPLOS_POR_REGLA])
                texto += f" (ejemplos: {muestra})"
            if resultado.get('excepcion'):
                texto += f". Excepción: {resultado['excepcion']}"
            lineas.append(texto)
    return lineas


def construir_resumen(info, reportes_calidad, carga):
    """Texto Markdown del resumen de la corrida."""
    lineas = [
        f"# Evidencia de la corrida `{info['run_id']}`",
        '',
        f"- Lote: `{info['id']}` (creado {info['creado_utc']})",
        f"- Resultado: {'cargada en el warehouse' if carga else 'detenida antes de la carga'}",
        '',
        '## Compuertas de calidad (Great Expectations)',
        '',
        '| Compuerta | Nombre | Estado | Reglas evaluadas | Críticas fallidas | Advertencias |',
        '|---:|---|---|---:|---|---|',
        *[_fila_compuerta(r) for r in reportes_calidad],
    ]
    pendientes = 3 - len(reportes_calidad)
    if pendientes > 0:
        texto = 'compuerta no se ejecutó' if pendientes == 1 else 'compuertas no se ejecutaron'
        lineas += ['', f'{pendientes} {texto}: la corrida se detuvo antes.']
    fallas = _detalle_fallas(reportes_calidad)
    if fallas:
        lineas += ['', '### Expectativas no cumplidas', '', *fallas]
    metricas = {k: v for r in reportes_calidad for k, v in r.get('metricas', {}).items()}
    if metricas:
        lineas += ['', '### Métricas de conciliación', '', '| Métrica | Valor |', '|---|---:|']
        lineas += [f'| {k} | {_numero(v, 4) if isinstance(v, float) else v} |' for k, v in metricas.items()]
    if carga:
        lineas += [
            '', '## Carga en el warehouse', '',
            f"- Estrategia: {carga['estrategia']}",
            f"- Estado de la compuerta 3 al cargar: {carga['estado_compuerta_3']}",
            '', '| Tabla | Filas |', '|---|---:|',
            *[f'| {tabla} | {_numero(filas)} |' for tabla, filas in carga['filas_por_tabla'].items()],
            f"| **Total** | **{_numero(carga['total_filas'])}** |",
        ]
        reiniciadas = carga.get('tablas_streaming_reiniciadas')
        if reiniciadas:
            lineas += ['', f"Tablas del streaming reiniciadas por la carga: {', '.join(reiniciadas)}."]
    lineas += ['', 'Generado con `python scripts/exportar_evidencia.py`. Los JSON de esta carpeta son '
               'copias exactas (verificadas por sha256) de los del lote.', '']
    return '\n'.join(lineas)


def exportar(id_lote=None, nombre='corrida', destino=None):
    """Copia los reportes del lote a <destino>/<nombre>/ y escribe resumen.md.

    Reemplaza el contenido previo de esa carpeta. Devuelve la ruta de la carpeta.
    """
    info = elegir_lote(id_lote)
    reportes = _reportes(info)
    carpeta = Path(destino or DESTINO_POR_DEFECTO) / nombre
    if carpeta.exists():
        shutil.rmtree(carpeta)
    carpeta.mkdir(parents=True)
    shutil.copyfile(info['ruta'] / 'lote.json', carpeta / 'lote.json')
    calidad, carga = [], None
    for fuente, relativa, ruta in reportes:
        shutil.copyfile(ruta, carpeta / Path(relativa).name)
        if fuente == 'calidad':
            calidad.append(lotes.leer_json(ruta))
        else:
            carga = lotes.leer_json(ruta)
    (carpeta / 'resumen.md').write_text(construir_resumen(info, calidad, carga), encoding='utf-8')
    return carpeta
