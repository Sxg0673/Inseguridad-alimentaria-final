"""
Escrituras del consumidor en el Data Warehouse (tablas del streaming).

Cada mensaje se procesa en UNA transaccion (src/streaming/procesamiento.py):
staging, hecho, snapshot y estado se confirman juntos o no se confirma
nada. Kafka solo recibe la confirmacion del offset despues del commit de la
base, asi que un corte a mitad de un mensaje hace que Kafka lo reentregue y
se procese completo, sin perdidas ni duplicados.

Las funciones reciben un cursor de psycopg2 dentro de esa transaccion.
"""
from src.streaming import contrato as c

SQL_INSERTAR_CRUDO = """
INSERT INTO stg_evento_encuesta (topic, particion, desplazamiento, clave, contenido)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (topic, particion, desplazamiento) DO NOTHING
RETURNING id_evento_crudo
"""

SQL_BUSCAR_CRUDO = """
SELECT id_evento_crudo, estado FROM stg_evento_encuesta
WHERE topic = %s AND particion = %s AND desplazamiento = %s
"""

SQL_MARCAR = """
UPDATE stg_evento_encuesta SET estado = %s, motivo = %s, procesado_en = now()
WHERE id_evento_crudo = %s
"""

# Una sola consulta resuelve las tres referencias por clave natural. Las
# dimensiones son pequenas y se consultan en cada evento (no se cachean):
# una carga nueva del DW cambia las claves sustitutas.
SQL_REFERENCIAS = """
SELECT
    (SELECT id_geografia FROM dim_geografia
     WHERE codigo_departamento = %(departamento)s AND codigo_region = %(region)s AND codigo_clase = %(clase)s),
    (SELECT id_nivel_severidad_ia FROM dim_nivel_severidad_ia WHERE nombre_severidad = %(severidad)s),
    (SELECT id_departamento FROM dim_departamento WHERE codigo_departamento = %(departamento)s)
"""

# ON CONFLICT DO NOTHING sin columna: cubre el hogar repetido
# (directorio, orden) y el id_evento repetido. Si no inserta, es DUPLICADO.
SQL_INSERTAR_HECHO = """
INSERT INTO fact_evento_encuesta (
    id_evento_crudo, id_evento, directorio, orden, id_geografia, id_nivel_severidad_ia,
    factor_expansion, prob_ia_moderada_grave, prob_ia_grave,
    momento_evento, momento_procesado, latencia_ms)
VALUES (%(id_crudo)s, %(id_evento)s, %(directorio)s, %(orden)s, %(id_geografia)s, %(id_severidad)s,
        %(factor)s, %(prob_mg)s, %(prob_g)s, %(momento_evento)s, %(momento_procesado)s, %(latencia_ms)s)
ON CONFLICT DO NOTHING
RETURNING id_evento_encuesta
"""

# Snapshot acumulado: solo conteos y sumas aditivas.
SQL_ACTUALIZAR_METRICA = """
INSERT INTO fact_metrica_departamento_stream AS m (
    id_departamento, hogares_recibidos, hogares_con_fies, hogares_grave,
    suma_fex_con_fies, suma_fex_prob_mg, suma_fex_prob_g, actualizado_en)
VALUES (%(id_departamento)s, 1, %(con_fies)s, %(grave)s, %(fex_con_fies)s, %(fex_prob_mg)s, %(fex_prob_g)s, now())
ON CONFLICT (id_departamento) DO UPDATE SET
    hogares_recibidos = m.hogares_recibidos + 1,
    hogares_con_fies = m.hogares_con_fies + EXCLUDED.hogares_con_fies,
    hogares_grave = m.hogares_grave + EXCLUDED.hogares_grave,
    suma_fex_con_fies = m.suma_fex_con_fies + EXCLUDED.suma_fex_con_fies,
    suma_fex_prob_mg = m.suma_fex_prob_mg + EXCLUDED.suma_fex_prob_mg,
    suma_fex_prob_g = m.suma_fex_prob_g + EXCLUDED.suma_fex_prob_g,
    actualizado_en = now()
"""


def guardar_crudo(cursor, topic, particion, desplazamiento, clave, contenido):
    """Guarda el mensaje tal como llego. Devuelve (id_evento_crudo, ya_procesado).

    ya_procesado es True si Kafka reentrego un mensaje que ya se habia
    procesado y confirmado en la base (no se vuelve a procesar).
    """
    texto_clave = None if clave is None else bytes(clave).decode('utf-8', errors='replace')
    texto = None if contenido is None else bytes(contenido).decode('utf-8', errors='replace')
    cursor.execute(SQL_INSERTAR_CRUDO, (topic, particion, desplazamiento, texto_clave, texto))
    fila = cursor.fetchone()
    if fila is not None:
        return fila[0], False
    cursor.execute(SQL_BUSCAR_CRUDO, (topic, particion, desplazamiento))
    id_crudo, estado = cursor.fetchone()
    return id_crudo, estado != c.RECIBIDO


def marcar(cursor, id_crudo, estado, motivo=None):
    cursor.execute(SQL_MARCAR, (estado, motivo, id_crudo))


def resolver_referencias(cursor, evento):
    """Claves sustitutas del evento. Devuelve (referencias, errores)."""
    cursor.execute(SQL_REFERENCIAS, {
        'departamento': evento[c.DEPARTAMENTO], 'region': evento[c.REGION],
        'clase': evento[c.CLASE], 'severidad': evento[c.SEVERIDAD],
    })
    id_geografia, id_severidad, id_departamento = cursor.fetchone()
    errores = []
    if id_geografia is None:
        errores.append(f'no existe en dim_geografia: departamento {evento[c.DEPARTAMENTO]!r}, '
                       f'region {evento[c.REGION]!r}, clase {evento[c.CLASE]!r}')
    if id_severidad is None:
        errores.append(f'no existe en dim_nivel_severidad_ia: {evento[c.SEVERIDAD]!r}')
    if id_departamento is None:
        errores.append(f'no existe en dim_departamento: {evento[c.DEPARTAMENTO]!r}')
    referencias = {'id_geografia': id_geografia, 'id_severidad': id_severidad, 'id_departamento': id_departamento}
    return referencias, errores


def guardar_evento_valido(cursor, id_crudo, evento, referencias, momento_procesado):
    """Inserta el hecho y actualiza el snapshot. Devuelve False si el hogar ya estaba (DUPLICADO)."""
    latencia = (momento_procesado - evento[c.MOMENTO_EVENTO]).total_seconds() * 1000
    cursor.execute(SQL_INSERTAR_HECHO, {
        'id_crudo': id_crudo, 'id_evento': evento[c.ID_EVENTO],
        'directorio': evento[c.DIRECTORIO], 'orden': evento[c.ORDEN],
        'id_geografia': referencias['id_geografia'], 'id_severidad': referencias['id_severidad'],
        'factor': evento[c.FACTOR], 'prob_mg': evento[c.PROB_MG], 'prob_g': evento[c.PROB_G],
        'momento_evento': evento[c.MOMENTO_EVENTO], 'momento_procesado': momento_procesado,
        # Relojes del mismo equipo; si el del productor va adelantado, no se guarda negativa
        'latencia_ms': max(0, round(latencia)),
    })
    if cursor.fetchone() is None:
        return False
    factor = evento[c.FACTOR]
    con_fies = evento[c.PROB_MG] is not None
    cursor.execute(SQL_ACTUALIZAR_METRICA, {
        'id_departamento': referencias['id_departamento'],
        'con_fies': int(con_fies),
        'grave': int(evento[c.SEVERIDAD] == c.SEVERIDAD_GRAVE),
        'fex_con_fies': factor if con_fies else 0,
        'fex_prob_mg': factor * evento[c.PROB_MG] if con_fies else 0,
        'fex_prob_g': factor * evento[c.PROB_G] if evento[c.PROB_G] is not None else 0,
    })
    return True
