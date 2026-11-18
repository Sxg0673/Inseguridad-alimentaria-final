"""
Origen de los eventos: el hecho de hogares del Data Warehouse.

El productor re-publica fact_seguridad_alimentaria_hogar como llegada simulada
de encuestas. Lee el hecho UNA sola vez al arrancar (con su geografia y su
severidad) y publica desde esa copia en memoria: si el DAG recarga el DW
mientras se publica, la lectura sigue siendo la de una sola carga.

El evento no usa id_hogar (clave sustituta que se reconstruye en cada
carga) sino la clave natural del hogar: directorio + orden.
"""
import random

CONSULTA_HOGARES = """
SELECT
    f.directorio,
    f.orden,
    g.codigo_departamento,
    g.codigo_region,
    g.codigo_clase,
    s.nombre_severidad,
    f.factor_expansion,
    f.prob_ia_moderada_grave,
    f.prob_ia_grave
FROM fact_seguridad_alimentaria_hogar f
JOIN dim_geografia g ON g.id_geografia = f.id_geografia
JOIN dim_nivel_severidad_ia s ON s.id_nivel_severidad_ia = f.id_nivel_severidad_ia
ORDER BY f.directorio, f.orden
"""


def leer_hogares(conexion):
    """Todas las filas del hecho de hogares como diccionarios (orden estable)."""
    with conexion.cursor() as cursor:
        cursor.execute(CONSULTA_HOGARES)
        columnas = [descripcion[0] for descripcion in cursor.description]
        filas = [dict(zip(columnas, fila, strict=True)) for fila in cursor.fetchall()]
    conexion.rollback()  # solo lectura: no deja una transaccion abierta
    return filas


def ordenar_para_envio(hogares, semilla, limite=None):
    """Mezcla los hogares de forma reproducible y, si se pide, toma los primeros N.

    Se mezclan para que lleguen departamentos variados, como en un operativo
    de campo real, y no un departamento completo tras otro. Misma semilla =
    mismo orden y misma muestra.
    """
    mezclados = list(hogares)
    random.Random(semilla).shuffle(mezclados)
    return mezclados if limite is None else mezclados[:limite]
