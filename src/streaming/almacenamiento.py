"""
Escrituras del consumidor en el Data Warehouse (tablas del streaming), por lotes.

El consumidor procesa micro-lotes en UNA transaccion (src/streaming/procesamiento.py).
Aqui cada paso se hace con una sola sentencia para todo el lote: en Docker para
Mac cada ida y vuelta a PostgreSQL cuesta milisegundos, y con 5 sentencias por
mensaje el consumidor no pasaba de ~50 eventos/s (medido). Por lote son 6
sentencias, sin importar cuantos mensajes traiga.

Las funciones reciben un cursor de psycopg2 dentro de esa transaccion.
"""
from collections import defaultdict

from psycopg2.extras import execute_values

from src.streaming import contrato as c


def _texto(valor):
    return None if valor is None else bytes(valor).decode('utf-8', errors='replace')


def guardar_crudos(cursor, mensajes):
    """Guarda los mensajes tal como llegaron. Devuelve {(topic, particion, desplazamiento): (id, ya_procesado)}.

    ya_procesado es True si Kafka reentrego un mensaje que ya se habia procesado
    y confirmado en la base (no se vuelve a procesar).
    """
    filas = [(m.topic(), m.partition(), m.offset(), _texto(m.key()), _texto(m.value())) for m in mensajes]
    nuevos = execute_values(cursor, """
        INSERT INTO stg_evento_encuesta (topic, particion, desplazamiento, clave, contenido) VALUES %s
        ON CONFLICT (topic, particion, desplazamiento) DO NOTHING
        RETURNING topic, particion, desplazamiento, id_evento_crudo""", filas, fetch=True)
    resultado = {(t, p, d): (id_crudo, False) for t, p, d, id_crudo in nuevos}
    faltantes = [(t, p, d) for t, p, d, _, _ in filas if (t, p, d) not in resultado]
    if faltantes:  # reentregas: solo ocurre al reiniciar tras un corte
        existentes = execute_values(cursor, """
            SELECT s.topic, s.particion, s.desplazamiento, s.id_evento_crudo, s.estado
            FROM stg_evento_encuesta s JOIN (VALUES %s) AS v (topic, particion, desplazamiento)
              ON s.topic = v.topic AND s.particion = v.particion AND s.desplazamiento = v.desplazamiento""",
                                    faltantes, fetch=True)
        for t, p, d, id_crudo, estado in existentes:
            resultado[(t, p, d)] = (id_crudo, estado != c.RECIBIDO)
    return resultado


def leer_dimensiones(cursor):
    """Claves sustitutas de las dimensiones por clave natural.

    Se leen en cada lote (son pequenas): una carga nueva del DW cambia las claves.
    """
    cursor.execute('SELECT codigo_departamento, codigo_region, codigo_clase, id_geografia FROM dim_geografia')
    geografia = {(d, r, k): i for d, r, k, i in cursor.fetchall()}
    cursor.execute('SELECT nombre_severidad, id_nivel_severidad_ia FROM dim_nivel_severidad_ia')
    severidad = dict(cursor.fetchall())
    cursor.execute('SELECT codigo_departamento, id_departamento FROM dim_departamento')
    departamento = dict(cursor.fetchall())
    return geografia, severidad, departamento


def resolver_referencias(dimensiones, evento):
    """Claves sustitutas de un evento. Devuelve (referencias, errores)."""
    geografia, severidad, departamento = dimensiones
    clave_geo = (evento[c.DEPARTAMENTO], evento[c.REGION], evento[c.CLASE])
    referencias = {
        'id_geografia': geografia.get(clave_geo),
        'id_severidad': severidad.get(evento[c.SEVERIDAD]),
        'id_departamento': departamento.get(evento[c.DEPARTAMENTO]),
    }
    errores = []
    if referencias['id_geografia'] is None:
        errores.append(f'no existe en dim_geografia: departamento {clave_geo[0]!r}, region {clave_geo[1]!r}, '
                       f'clase {clave_geo[2]!r}')
    if referencias['id_severidad'] is None:
        errores.append(f'no existe en dim_nivel_severidad_ia: {evento[c.SEVERIDAD]!r}')
    if referencias['id_departamento'] is None:
        errores.append(f'no existe en dim_departamento: {evento[c.DEPARTAMENTO]!r}')
    return referencias, errores


def guardar_hechos(cursor, validos, momento_procesado):
    """Inserta los eventos validos en el hecho. Devuelve los id_evento_crudo insertados.

    validos: lista de (id_crudo, evento, referencias), en orden de llegada.
    ON CONFLICT DO NOTHING sin columna cubre el hogar repetido (directorio, orden)
    y el id_evento repetido, tambien dentro del mismo lote (gana el primero).
    Los que no se insertan son DUPLICADO.
    """
    if not validos:
        return set()
    filas = []
    for id_crudo, evento, ref in validos:
        latencia = (momento_procesado - evento[c.MOMENTO_EVENTO]).total_seconds() * 1000
        filas.append((id_crudo, evento[c.ID_EVENTO], evento[c.DIRECTORIO], evento[c.ORDEN], ref['id_geografia'],
                      ref['id_severidad'], evento[c.FACTOR], evento[c.PROB_MG], evento[c.PROB_G],
                      evento[c.MOMENTO_EVENTO], momento_procesado,
                      max(0, round(latencia))))  # relojes del mismo equipo: nunca negativa
    insertados = execute_values(cursor, """
        INSERT INTO fact_evento_encuesta (
            id_evento_crudo, id_evento, directorio, orden, id_geografia, id_nivel_severidad_ia,
            factor_expansion, prob_ia_moderada_grave, prob_ia_grave,
            momento_evento, momento_procesado, latencia_ms)
        VALUES %s
        ON CONFLICT DO NOTHING
        RETURNING id_evento_crudo""", filas, template='(%s, %s::uuid, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                                fetch=True, page_size=len(filas))
    return {fila[0] for fila in insertados}


def actualizar_metricas(cursor, insertados):
    """Suma al snapshot por departamento los eventos insertados. insertados: lista de (evento, referencias)."""
    if not insertados:
        return
    suma = defaultdict(lambda: [0, 0, 0, 0.0, 0.0, 0.0])
    for evento, ref in insertados:
        fila = suma[ref['id_departamento']]
        con_fies = evento[c.PROB_MG] is not None
        fila[0] += 1
        fila[1] += int(con_fies)
        fila[2] += int(evento[c.SEVERIDAD] == c.SEVERIDAD_GRAVE)
        fila[3] += evento[c.FACTOR] if con_fies else 0
        fila[4] += evento[c.FACTOR] * evento[c.PROB_MG] if con_fies else 0
        fila[5] += evento[c.FACTOR] * evento[c.PROB_G] if evento[c.PROB_G] is not None else 0
    # Una fila por departamento: ON CONFLICT DO UPDATE no admite tocar la misma fila dos veces
    execute_values(cursor, """
        INSERT INTO fact_metrica_departamento_stream AS m (
            id_departamento, hogares_recibidos, hogares_con_fies, hogares_grave,
            suma_fex_con_fies, suma_fex_prob_mg, suma_fex_prob_g, actualizado_en)
        VALUES %s
        ON CONFLICT (id_departamento) DO UPDATE SET
            hogares_recibidos = m.hogares_recibidos + EXCLUDED.hogares_recibidos,
            hogares_con_fies = m.hogares_con_fies + EXCLUDED.hogares_con_fies,
            hogares_grave = m.hogares_grave + EXCLUDED.hogares_grave,
            suma_fex_con_fies = m.suma_fex_con_fies + EXCLUDED.suma_fex_con_fies,
            suma_fex_prob_mg = m.suma_fex_prob_mg + EXCLUDED.suma_fex_prob_mg,
            suma_fex_prob_g = m.suma_fex_prob_g + EXCLUDED.suma_fex_prob_g,
            actualizado_en = now()""",
                   [(id_depto, *valores) for id_depto, valores in suma.items()],
                   template='(%s, %s, %s, %s, %s, %s, %s, now())')


def marcar_estados(cursor, estados):
    """Estado y motivo final de cada mensaje. estados: lista de (id_crudo, estado, motivo)."""
    if not estados:
        return
    execute_values(cursor, """
        UPDATE stg_evento_encuesta AS s SET estado = v.estado, motivo = v.motivo, procesado_en = now()
        FROM (VALUES %s) AS v (id_evento_crudo, estado, motivo)
        WHERE s.id_evento_crudo = v.id_evento_crudo""", estados,
                   template='(%s::bigint, %s::text, %s::text)', page_size=len(estados))
