"""Flujo completo del streaming contra PostgreSQL real, con Kafka en memoria.

Recorre el camino de la entrega final: DW (hecho de hogares) -> productor ->
topic -> consumidor (contrato, referencias, duplicados) -> hecho y snapshot
-> vistas del dashboard. Usa los scripts reales de streaming/.

Seguridad: cada prueba crea un ESQUEMA propio (prueba_streaming_<aleatorio>)
en la base de ETL_DATABASE_URL, trabaja solo ahi y lo borra al terminar. No
toca las tablas del esquema public, asi que se puede correr contra el
warehouse del equipo:

    pytest -m warehouse tests/test_streaming_flujo.py

Kafka real (red, rebalanceos, reinicio del broker) se prueba con el
docker-compose: README, seccion 11.
"""
import csv
import importlib.util
import os
import random
import uuid
from decimal import Decimal
from pathlib import Path

import pytest

from kafka_en_memoria import BrokerEnMemoria, ConsumidorEnMemoria, ProductorEnMemoria
from src.streaming import almacenamiento, contrato as c, fuente
from src.streaming.procesamiento import YA_PROCESADO, Parada, consumir
from src.streaming.publicacion import Estadisticas, construir_plan, enviar

pytestmark = [pytest.mark.warehouse]

RAIZ = Path(__file__).resolve().parents[1]
TOPIC = 'encuestas-hogares'
GRUPO = 'consumidor-encuestas'
SEVERIDADES = {  # ids a proposito distintos del orden, para detectar un mapeo equivocado
    'Seguridad alimentaria': 14, 'Inseguridad leve': 11, 'Inseguridad moderada': 13,
    'Inseguridad grave': 10, 'Sin dato': 12,
}
DEPTO_ALTO = '27'      # Choco: prevalencia alta a proposito (debe quedar en alerta)
DEPTO_PEQUENO = '94'   # Guainia: muy pocos hogares (debe quedar sin_muestra)


def _cargar_script(nombre):
    spec = importlib.util.spec_from_file_location(f'script_{nombre}', RAIZ / 'streaming' / f'{nombre}.py')
    modulo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(modulo)
    return modulo


def _url_con_esquema(url, esquema):
    separador = '&' if '?' in url else '?'
    return f'{url}{separador}options=-csearch_path%3D{esquema}'


@pytest.fixture
def base(monkeypatch):
    """Conexion a un esquema nuevo con el DDL y las vistas del proyecto. Se borra al final."""
    psycopg2 = pytest.importorskip('psycopg2')
    url = os.getenv('ETL_DATABASE_URL')
    if not url:
        pytest.skip('Define ETL_DATABASE_URL para probar el flujo del streaming')
    esquema = f'prueba_streaming_{uuid.uuid4().hex[:8]}'
    try:
        administrador = psycopg2.connect(url, connect_timeout=5)
    except psycopg2.OperationalError:
        pytest.skip('PostgreSQL no esta disponible')
    administrador.autocommit = True
    with administrador.cursor() as cursor:
        cursor.execute(f'CREATE SCHEMA {esquema}')
    url_esquema = _url_con_esquema(url, esquema)
    monkeypatch.setenv('ETL_DATABASE_URL', url_esquema)  # para los scripts de streaming/
    conexion = psycopg2.connect(url_esquema)
    with conexion.cursor() as cursor:
        cursor.execute((RAIZ / 'sql' / 'create_tables.sql').read_text(encoding='utf-8'))
        cursor.execute((RAIZ / 'sql' / 'vistas_streaming.sql').read_text(encoding='utf-8'))
    conexion.commit()
    try:
        yield conexion
    finally:
        conexion.close()
        with administrador.cursor() as cursor:
            cursor.execute(f'DROP SCHEMA {esquema} CASCADE')
        administrador.close()


def _insertar_por_defecto(cursor, tabla, valores):
    """Inserta una fila llenando con False/0 las columnas no indicadas (dimensiones junk)."""
    cursor.execute('SELECT column_name, data_type FROM information_schema.columns '
                   'WHERE table_name = %s AND table_schema = current_schema() ORDER BY ordinal_position', (tabla,))
    columnas = cursor.fetchall()
    fila = [valores.get(n, False if t == 'boolean' else 0) for n, t in columnas]
    cursor.execute(f"INSERT INTO {tabla} VALUES ({', '.join(['%s'] * len(fila))})", fila)


def poblar_dw(conexion, hogares_por_depto=60, semilla=7):
    """DW sintetico: los 33 departamentos DIVIPOLA, dos clases por departamento y hogares coherentes con C12."""
    with open(RAIZ / 'data' / 'reference' / 'divipola_departamentos.csv', encoding='utf-8') as archivo:
        departamentos = [(f['Código Departamento'], f['Nombre Departamento']) for f in csv.DictReader(archivo)]
    azar = random.Random(semilla)
    with conexion.cursor() as cursor:
        geografia = {}
        for numero, (codigo, nombre) in enumerate(departamentos, start=1):
            cursor.execute('INSERT INTO dim_departamento VALUES (%s, %s, %s)', (100 + numero, codigo, nombre))
            for clase in ('1', '2'):
                id_geo = 1000 + numero * 10 + int(clase)
                region = '8' if codigo == '11' and clase == '1' else str(1 + numero % 6)  # Bogota: region segun clase
                cursor.execute('INSERT INTO dim_geografia VALUES (%s,%s,%s,%s,%s,%s,%s)',
                               (id_geo, codigo, nombre, region, f'Region {region}', clase, f'Clase {clase}'))
                geografia[(codigo, clase)] = id_geo
        for nombre, id_sev in SEVERIDADES.items():
            cursor.execute('INSERT INTO dim_nivel_severidad_ia VALUES (%s, %s, NULL, NULL)', (id_sev, nombre))
        cursor.execute("INSERT INTO dim_nivel_ingreso VALUES (1, 'Q1', 'Q1')")
        _insertar_por_defecto(cursor, 'dim_choque_economico', {'id_choque_economico': 1})
        _insertar_por_defecto(cursor, 'dim_estrategia_afrontamiento', {'id_estrategia_afrontamiento': 1})
        id_hogar = 0
        for codigo, _ in departamentos:
            cantidad = 12 if codigo == DEPTO_PEQUENO else hogares_por_depto
            for _ in range(cantidad):
                id_hogar += 1
                severidad = azar.choices(list(SEVERIDADES), weights=[40, 30, 15, 10, 5])[0]
                if severidad == 'Sin dato':
                    mg = g = None
                else:
                    techo = 95 if codigo == DEPTO_ALTO else 40
                    mg = round(azar.uniform(0, techo), 6)
                    g = round(azar.uniform(0, mg / 3), 6)
                if codigo == DEPTO_ALTO and mg is not None:
                    mg = round(azar.uniform(70, 99), 6)
                    g = round(azar.uniform(0, mg / 3), 6)
                cursor.execute(
                    'INSERT INTO fact_seguridad_alimentaria_hogar VALUES (%s,%s,%s,%s,1,%s,1,1,true,true,%s,%s,%s)',
                    (id_hogar, f'{7000000 + id_hogar}', '1', geografia[(codigo, azar.choice('12'))],
                     SEVERIDADES[severidad], round(azar.uniform(50, 900), 6), mg, g))
        # Como en la carga real: las vistas materializadas se calculan con el DW ya cargado
        cursor.execute('REFRESH MATERIALIZED VIEW mv_stream_referencia_nacional')
        cursor.execute('REFRESH MATERIALIZED VIEW mv_stream_esperados_departamento')
    conexion.commit()
    return id_hogar


def publicar(broker, hogares, con_invalidos=False):
    plan = construir_plan(hogares, con_invalidos)
    estadisticas = Estadisticas(total=len(plan))
    enviar(ProductorEnMemoria(broker), TOPIC, plan, None, estadisticas)
    return plan


def consumir_todo(conexion, broker, **kwargs):
    parada = Parada()
    consumidor = ConsumidorEnMemoria(broker, GRUPO, parada, **kwargs)
    return consumir(consumidor, conexion, TOPIC, parada, tiempo_inactivo=0.05)


def una_fila(conexion, consulta):
    with conexion.cursor() as cursor:
        cursor.execute(consulta)
        columnas = [d[0] for d in cursor.description]
        fila = dict(zip(columnas, cursor.fetchone(), strict=True))
    conexion.rollback()
    return fila


def filas(conexion, consulta):
    with conexion.cursor() as cursor:
        cursor.execute(consulta)
        columnas = [d[0] for d in cursor.description]
        resultado = [dict(zip(columnas, f, strict=True)) for f in cursor.fetchall()]
    conexion.rollback()
    return resultado


# ---------------------------------------------------------------------------
def test_flujo_completo_por_los_scripts_es_coherente_con_el_dw(base):
    """Replay completo (--completo) por los scripts reales: la prevalencia del streaming = la del DW."""
    total = poblar_dw(base)
    broker = BrokerEnMemoria(TOPIC)
    productor = _cargar_script('productor')
    consumidor = _cargar_script('consumidor')

    assert productor.main(['--completo'], crear=lambda _s: ProductorEnMemoria(broker)) == 0
    assert len(broker.mensajes()) == total
    assert consumidor.main(['--tiempo-inactivo', '0.2'], instalar_senales=False,
                           crear=lambda _s, grupo, _e: ConsumidorEnMemoria(broker, grupo)) == 0

    nacional = una_fila(base, 'SELECT * FROM vw_stream_nacional')
    assert nacional['hogares_esperados'] == nacional['hogares_recibidos'] == total
    assert nacional['avance_pct'] == Decimal('100.00')
    assert nacional['prevalencia_moderada_grave_pct'] == nacional['referencia_nacional_pct']   # prueba de coherencia I8

    # Por departamento, igual que calcular directo sobre el DW (como las vistas R1-R7)
    directo = {f['codigo_departamento']: f['prev'] for f in filas(base, """
        SELECT g.codigo_departamento,
               ROUND((SUM(f.factor_expansion * f.prob_ia_moderada_grave) / SUM(f.factor_expansion))::numeric, 2) AS prev
        FROM fact_seguridad_alimentaria_hogar f JOIN dim_geografia g USING (id_geografia)
        WHERE f.prob_ia_moderada_grave IS NOT NULL GROUP BY 1""")}
    departamentos = filas(base, 'SELECT * FROM vw_stream_departamento')
    assert len(departamentos) == 33
    for d in departamentos:
        assert d['avance_pct'] == Decimal('100.00')
        assert d['prevalencia_moderada_grave_pct'] == directo[d['codigo_departamento']]

    salud = una_fila(base, 'SELECT * FROM vw_stream_salud_flujo')
    assert salud['mensajes'] == salud['validos'] == total
    assert salud['pendientes'] == 0 and salud['latencia_media_ms'] >= 0


def test_alerta_temprana_y_muestra_minima(base):
    poblar_dw(base)
    broker = BrokerEnMemoria(TOPIC)
    hogares = fuente.leer_hogares(base)
    publicar(broker, fuente.ordenar_para_envio(hogares, 42))
    consumir_todo(base, broker)
    estados = {d['codigo_departamento']: d for d in filas(base, 'SELECT * FROM vw_stream_departamento')}
    assert estados[DEPTO_ALTO]['estado_alerta'] == 'alerta'
    assert estados[DEPTO_ALTO]['limite_inferior_ic95_pct'] > estados[DEPTO_ALTO]['referencia_nacional_pct']
    assert estados[DEPTO_PEQUENO]['estado_alerta'] == 'sin_muestra'          # menos de 30 hogares con FIES
    normales = [d for d in estados.values() if d['estado_alerta'] == 'normal']
    assert normales and all(d['hogares_con_fies'] >= 30 for d in normales)
    assert una_fila(base, 'SELECT departamentos_en_alerta FROM vw_stream_nacional')['departamentos_en_alerta'] >= 1


def test_eventos_invalidos_no_detienen_el_consumidor_y_quedan_registrados(base):
    poblar_dw(base)
    broker = BrokerEnMemoria(TOPIC)
    hogares = fuente.ordenar_para_envio(fuente.leer_hogares(base), 42, limite=200)
    publicar(broker, hogares, con_invalidos=True)
    conteos = consumir_todo(base, broker)

    assert conteos == {c.VALIDO: 200, c.INVALIDO_ESQUEMA: 3, c.INVALIDO_VALORES: 1,
                       c.INVALIDO_REFERENCIA: 1, c.DUPLICADO: 1}
    salud = una_fila(base, 'SELECT * FROM vw_stream_salud_flujo')
    assert (salud['mensajes'], salud['validos'], salud['invalidos_esquema'], salud['invalidos_valores'],
            salud['invalidos_referencia'], salud['duplicados']) == (206, 200, 3, 1, 1, 1)
    sin_motivo = una_fila(base, "SELECT count(*) AS n FROM stg_evento_encuesta "
                                "WHERE estado <> 'VALIDO' AND motivo IS NULL")
    assert sin_motivo['n'] == 0
    assert una_fila(base, 'SELECT count(*) AS n FROM fact_evento_encuesta')['n'] == 200
    # el JSON cortado se conserva tal como llego
    cortado = una_fila(base, "SELECT contenido FROM stg_evento_encuesta WHERE motivo LIKE 'JSON mal formado%'")
    assert not cortado['contenido'].endswith('}')


def test_consumidor_detenido_retoma_sin_perdidas_ni_duplicados(base):
    poblar_dw(base)
    broker = BrokerEnMemoria(TOPIC)
    publicar(broker, fuente.ordenar_para_envio(fuente.leer_hogares(base), 42, limite=300))
    primera = consumir_todo(base, broker, detener_tras=120)   # Ctrl+C a mitad
    assert sum(primera.values()) == 120
    assert una_fila(base, 'SELECT hogares_recibidos FROM vw_stream_nacional')['hogares_recibidos'] == 120
    segunda = consumir_todo(base, broker)                     # se vuelve a arrancar
    assert sum(segunda.values()) == 180
    nacional = una_fila(base, 'SELECT hogares_recibidos FROM vw_stream_nacional')
    assert nacional['hogares_recibidos'] == 300
    assert una_fila(base, 'SELECT count(*) AS n FROM stg_evento_encuesta')['n'] == 300


def test_corte_entre_la_base_y_kafka_no_duplica(base):
    """La base confirmo el mensaje pero el offset no llego a Kafka: se reentrega y se omite."""
    poblar_dw(base)
    broker = BrokerEnMemoria(TOPIC)
    publicar(broker, fuente.ordenar_para_envio(fuente.leer_hogares(base), 42, limite=50))

    class CaeAlConfirmar(ConsumidorEnMemoria):
        def commit(self, message=None, asynchronous=False):
            if self.entregados == 20:
                raise RuntimeError('el proceso murio antes de confirmar el offset')
            super().commit(message=message)

    parada = Parada()
    with pytest.raises(RuntimeError):
        consumir(CaeAlConfirmar(broker, GRUPO, parada), base, TOPIC, parada, tiempo_inactivo=0.05)
    conteos = consumir_todo(base, broker)
    assert conteos[YA_PROCESADO] == 1
    assert una_fila(base, 'SELECT count(*) AS n FROM fact_evento_encuesta')['n'] == 50
    assert una_fila(base, 'SELECT hogares_recibidos FROM vw_stream_nacional')['hogares_recibidos'] == 50


def test_falla_de_la_base_a_mitad_de_un_mensaje_revierte_todo(base, monkeypatch):
    psycopg2 = pytest.importorskip('psycopg2')
    poblar_dw(base)
    broker = BrokerEnMemoria(TOPIC)
    publicar(broker, fuente.ordenar_para_envio(fuente.leer_hogares(base), 42, limite=40))
    original = almacenamiento.guardar_evento_valido
    llamadas = {'n': 0}

    def falla_en_la_decima(*args, **kwargs):
        llamadas['n'] += 1
        if llamadas['n'] == 10:
            raise psycopg2.OperationalError('conexion perdida con el warehouse')
        return original(*args, **kwargs)

    monkeypatch.setattr(almacenamiento, 'guardar_evento_valido', falla_en_la_decima)
    with pytest.raises(psycopg2.OperationalError):
        consumir_todo(base, broker)
    # el mensaje que fallo no dejo rastro (ni staging ni hecho): se revirtio completo
    assert una_fila(base, 'SELECT count(*) AS n FROM stg_evento_encuesta')['n'] == 9
    monkeypatch.setattr(almacenamiento, 'guardar_evento_valido', original)
    consumir_todo(base, broker)
    assert una_fila(base, 'SELECT count(*) AS n FROM fact_evento_encuesta')['n'] == 40
    assert una_fila(base, "SELECT count(*) AS n FROM stg_evento_encuesta WHERE estado = 'VALIDO'")['n'] == 40


def test_una_nueva_carga_del_dw_reinicia_el_streaming(base):
    from src.carga import TABLAS, TABLAS_STREAMING
    poblar_dw(base)
    broker = BrokerEnMemoria(TOPIC)
    publicar(broker, fuente.ordenar_para_envio(fuente.leer_hogares(base), 42, limite=30))
    consumir_todo(base, broker)
    with base.cursor() as cursor:
        cursor.execute(f"TRUNCATE TABLE {', '.join([t for _, t in TABLAS] + TABLAS_STREAMING)} RESTART IDENTITY")
    base.commit()
    salud = una_fila(base, 'SELECT mensajes FROM vw_stream_salud_flujo')
    assert salud['mensajes'] == 0


# ---------------------------------------------------------------------------
# Dashboard en tiempo real (streaming/dashboard.py) sobre las mismas vistas
# ---------------------------------------------------------------------------
def _correr_dashboard():
    testing = pytest.importorskip('streamlit.testing.v1')
    app = testing.AppTest.from_file(str(RAIZ / 'streaming' / 'dashboard.py'), default_timeout=60)
    app.run()
    assert not app.exception, [e.message for e in app.exception]
    return app


def test_dashboard_sin_eventos_no_falla(base):
    poblar_dw(base)
    app = _correr_dashboard()
    metricas = {m.label: m.value for m in app.metric}
    assert metricas['Hogares recibidos (RS1)'] == '0'
    assert any('Aún no llegan encuestas' in i.value for i in app.info)


def test_dashboard_muestra_las_metricas_del_streaming(base):
    poblar_dw(base)
    broker = BrokerEnMemoria(TOPIC)
    publicar(broker, fuente.ordenar_para_envio(fuente.leer_hogares(base), 42, limite=400), con_invalidos=True)
    consumir_todo(base, broker)
    nacional = una_fila(base, 'SELECT * FROM vw_stream_nacional')
    app = _correr_dashboard()
    metricas = {m.label: m.value for m in app.metric}
    assert metricas['Hogares recibidos (RS1)'] == '400'
    prevalencia = f"{nacional['prevalencia_moderada_grave_pct']:,.2f} %"
    assert metricas['Prevalencia acumulada (RS2)'] == prevalencia
    assert metricas['Departamentos en alerta (RS3)'] == str(nacional['departamentos_en_alerta'])
    assert metricas['Mensajes inválidos (RS4)'] == '6 de 406'
    assert len(app.dataframe) == 1 and len(app.dataframe[0].value) == 33
