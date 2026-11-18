"""
Precarga del streaming: encuestas ya recibidas antes de iniciar la demo.

Reproducir los 87.060 hogares por Kafka tarda decenas de minutos en un equipo
modesto (el consumidor procesa del orden de 75-90 eventos/s). Para una demo
realista, el operativo de campo "ya va avanzado": esta precarga copia un
porcentaje de los hogares directamente del DW a las tablas del streaming y
el productor publica por Kafka solo los que faltan (--pendientes).

- Mismo orden que el productor (misma semilla): la precarga toma los primeros
  N hogares y --pendientes publica el resto.
- Se marca origen = 'precarga' en fact_evento_encuesta: no tiene mensaje de
  Kafka, ni fila en el staging, ni latencia. Las metricas de salud del flujo
  solo miden lo que llego por Kafka.
- Respeta el modelo: las FK salen del mismo DW cargado y el snapshot
  (fact_metrica_departamento_stream) se calcula con las mismas sumas que el
  consumidor. No pasa por el contrato porque son datos que ya aprobaron las
  compuertas de Great Expectations del batch.
- Todo en una transaccion: o queda la precarga completa o no queda nada.
"""
import logging

from psycopg2.extras import execute_values

log = logging.getLogger('precarga')

SQL_INSERTAR_HECHOS = """
INSERT INTO fact_evento_encuesta (
    id_evento_crudo, id_evento, directorio, orden, id_geografia, id_nivel_severidad_ia,
    factor_expansion, prob_ia_moderada_grave, prob_ia_grave,
    momento_evento, momento_procesado, latencia_ms, origen)
SELECT NULL, gen_random_uuid(), f.directorio, f.orden, f.id_geografia, f.id_nivel_severidad_ia,
       f.factor_expansion, f.prob_ia_moderada_grave, f.prob_ia_grave,
       now(), now(), NULL, 'precarga'
FROM fact_seguridad_alimentaria_hogar f
JOIN tmp_precarga p ON p.directorio = f.directorio AND p.orden = f.orden
"""

# Mismas sumas que almacenamiento.SQL_ACTUALIZAR_METRICA, agregadas por departamento
SQL_SNAPSHOT = """
INSERT INTO fact_metrica_departamento_stream AS m (
    id_departamento, hogares_recibidos, hogares_con_fies, hogares_grave,
    suma_fex_con_fies, suma_fex_prob_mg, suma_fex_prob_g, actualizado_en)
SELECT d.id_departamento,
       COUNT(*),
       COUNT(*) FILTER (WHERE e.prob_ia_moderada_grave IS NOT NULL),
       COUNT(*) FILTER (WHERE s.nombre_severidad = 'Inseguridad grave'),
       COALESCE(SUM(e.factor_expansion) FILTER (WHERE e.prob_ia_moderada_grave IS NOT NULL), 0),
       COALESCE(SUM(e.factor_expansion * e.prob_ia_moderada_grave), 0),
       COALESCE(SUM(e.factor_expansion * e.prob_ia_grave), 0),
       now()
FROM fact_evento_encuesta e
JOIN dim_geografia g ON g.id_geografia = e.id_geografia
JOIN dim_departamento d ON d.codigo_departamento = g.codigo_departamento
JOIN dim_nivel_severidad_ia s ON s.id_nivel_severidad_ia = e.id_nivel_severidad_ia
WHERE e.origen = 'precarga'
GROUP BY d.id_departamento
ON CONFLICT (id_departamento) DO UPDATE SET
    hogares_recibidos = m.hogares_recibidos + EXCLUDED.hogares_recibidos,
    hogares_con_fies = m.hogares_con_fies + EXCLUDED.hogares_con_fies,
    hogares_grave = m.hogares_grave + EXCLUDED.hogares_grave,
    suma_fex_con_fies = m.suma_fex_con_fies + EXCLUDED.suma_fex_con_fies,
    suma_fex_prob_mg = m.suma_fex_prob_mg + EXCLUDED.suma_fex_prob_mg,
    suma_fex_prob_g = m.suma_fex_prob_g + EXCLUDED.suma_fex_prob_g,
    actualizado_en = now()
"""


class StreamingNoVacio(Exception):
    """La precarga solo se hace sobre una demo en cero."""


def precargar(conexion, hogares):
    """Copia los hogares indicados (directorio, orden) del DW a las tablas del streaming.

    Exige que el streaming este vacio (correr antes streaming/reiniciar_demo.py).
    Devuelve el numero de hogares precargados.
    """
    try:
        with conexion.cursor() as cursor:
            cursor.execute('SELECT count(*) FROM fact_evento_encuesta')
            if cursor.fetchone()[0]:
                raise StreamingNoVacio('El streaming ya tiene datos: correr primero python streaming/reiniciar_demo.py')
            cursor.execute('CREATE TEMP TABLE tmp_precarga (directorio TEXT, orden TEXT) ON COMMIT DROP')
            execute_values(cursor, 'INSERT INTO tmp_precarga VALUES %s',
                           [(h['directorio'], h['orden']) for h in hogares], page_size=5000)
            cursor.execute(SQL_INSERTAR_HECHOS)
            insertados = cursor.rowcount
            cursor.execute(SQL_SNAPSHOT)
        conexion.commit()
    except Exception:
        conexion.rollback()
        raise
    log.info('Precarga: %s hogares en fact_evento_encuesta y snapshot por departamento', f'{insertados:,}')
    return insertados


def hogares_recibidos(conexion):
    """Claves (directorio, orden) que ya estan en el streaming (por Kafka o por precarga)."""
    with conexion.cursor() as cursor:
        cursor.execute('SELECT directorio, orden FROM fact_evento_encuesta')
        claves = set(cursor.fetchall())
    conexion.rollback()
    return claves
