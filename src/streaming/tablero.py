"""
Consultas del dashboard en tiempo real (streaming/dashboard.py).

El dashboard solo lee las vistas del streaming (sql/vistas_streaming.sql).
Separadas aqui para poder probarlas sin Streamlit.

Vistas -> metricas (README, seccion 9):
    vw_stream_nacional        RS1 avance nacional, RS2 prevalencia acumulada y referencia del DW
    vw_stream_departamento    RS1-RS3 por departamento, con estado de alerta
    vw_stream_evolucion       RS2 en el tiempo (intervalos de 10 s)
    vw_stream_salud_flujo     RS4 estados de los mensajes y latencia
"""
import pandas as pd

CONSULTAS = {
    'nacional': 'SELECT * FROM vw_stream_nacional',
    'departamentos': 'SELECT * FROM vw_stream_departamento ORDER BY prevalencia_moderada_grave_pct DESC NULLS LAST',
    'evolucion': 'SELECT * FROM vw_stream_evolucion ORDER BY intervalo',
    'salud': 'SELECT * FROM vw_stream_salud_flujo',
}


def _leer(conexion, consulta):
    with conexion.cursor() as cursor:
        cursor.execute(consulta)
        columnas = [d[0] for d in cursor.description]
        datos = pd.DataFrame(cursor.fetchall(), columns=columnas)
    conexion.rollback()  # solo lectura: no deja transacciones abiertas que bloqueen la carga
    # Los DECIMAL de PostgreSQL llegan como Decimal: se pasan a float para graficar
    for columna in datos.columns:
        if datos[columna].dtype == object and datos[columna].map(lambda v: v.__class__.__name__ == 'Decimal').any():
            datos[columna] = pd.to_numeric(datos[columna])
    return datos


def leer_todo(conexion):
    """Las cuatro vistas en un diccionario de DataFrames."""
    return {nombre: _leer(conexion, consulta) for nombre, consulta in CONSULTAS.items()}


def estados_mensajes(salud):
    """Tabla larga de mensajes por estado, para el grafico de salud del flujo (RS4)."""
    fila = salud.iloc[0]
    estados = {
        'Validos': fila['validos'],
        'Invalido: esquema': fila['invalidos_esquema'],
        'Invalido: valores': fila['invalidos_valores'],
        'Invalido: referencia': fila['invalidos_referencia'],
        'Duplicados': fila['duplicados'],
    }
    return pd.DataFrame({'estado': list(estados), 'mensajes': [int(v) for v in estados.values()]})
