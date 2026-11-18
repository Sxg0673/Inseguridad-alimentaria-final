"""Tablas del streaming en el modelo y en la carga (entrega final).

Las pruebas sin marca leen los archivos SQL y el modulo de carga. La de
marca `warehouse` crea el esquema completo en una base real de PRUEBAS
(ETL_DATABASE_URL): no usarla contra el warehouse con datos.
"""
import os
import re
from pathlib import Path

import pytest

from src import carga

RAIZ = Path(__file__).resolve().parents[1]
DDL = (RAIZ / 'sql' / 'create_tables.sql').read_text(encoding='utf-8')
VISTAS_STREAMING = (RAIZ / 'sql' / 'vistas_streaming.sql').read_text(encoding='utf-8')


def test_tablas_del_streaming_existen_en_el_ddl():
    for tabla in carga.TABLAS_STREAMING:
        assert re.search(rf'CREATE TABLE IF NOT EXISTS {tabla}\b', DDL), tabla


def test_streaming_no_se_carga_desde_el_lote():
    cargadas = {tabla for _, tabla in carga.TABLAS}
    assert cargadas.isdisjoint(carga.TABLAS_STREAMING)


def test_hechos_del_streaming_tienen_fk_a_dimensiones_del_modelo():
    assert 'REFERENCES dim_geografia (id_geografia)' in DDL.split('fact_evento_encuesta (')[1]
    assert 'REFERENCES dim_nivel_severidad_ia' in DDL.split('fact_evento_encuesta (')[1]
    assert 'REFERENCES dim_departamento' in DDL.split('fact_metrica_departamento_stream (')[1]


@pytest.mark.warehouse
def test_truncate_de_la_carga_funciona_con_las_fk_del_streaming():
    psycopg2 = pytest.importorskip('psycopg2')
    url = os.getenv('ETL_DATABASE_URL')
    if not url:
        pytest.skip('Define ETL_DATABASE_URL (base de pruebas)')
    try:
        con = psycopg2.connect(url, connect_timeout=5)
    except psycopg2.OperationalError:
        pytest.skip('La base de pruebas no esta disponible')
    try:
        with con.cursor() as cur:
            cur.execute(DDL)
            cur.execute(VISTAS_STREAMING)
            todas = [t for _, t in carga.TABLAS] + carga.TABLAS_STREAMING
            cur.execute(f"TRUNCATE TABLE {', '.join(todas)} RESTART IDENTITY")
            cur.execute('SELECT count(*) FROM vw_stream_departamento')
            assert cur.fetchone()[0] == 0
    finally:
        con.rollback()
        con.close()
