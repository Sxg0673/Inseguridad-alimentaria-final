"""Dashboard en tiempo real del operativo de recoleccion (SIMULACION con la ECV 2025).

Lee las vistas del streaming en el warehouse y se refresca cada 2 segundos:
el consumidor de Kafka actualiza las tablas y este tablero muestra el cambio.
Es distinto del dashboard historico (Power BI, vistas R1-R7): aqui se ve el
estado ACUMULADO mientras llegan las encuestas, no el resultado final.

Uso (con el warehouse arriba y el DW cargado):
    streamlit run streaming/dashboard.py
"""
from datetime import datetime

import altair as alt
import psycopg2
import streamlit as st

from src.streaming import config, tablero

REFRESCO_S = 2
COLORES_ALERTA = alt.Scale(domain=['alerta', 'normal', 'sin_muestra'], range=['#c0392b', '#2e86c1', '#b3b6b7'])

st.set_page_config(page_title='Encuestas en tiempo real', layout='wide')
config.cargar_entorno()


@st.cache_resource
def conectar():
    conexion = psycopg2.connect(config.url_warehouse(), connect_timeout=5)
    conexion.set_session(readonly=True)
    return conexion


def leer():
    """Lee las vistas. Si la conexion guardada ya no sirve (p. ej. el warehouse se reinicio),
    la descarta y reintenta una vez; si vuelve a fallar, muestra el error y lo intenta en el
    siguiente refresco."""
    for intento in (1, 2):
        try:
            return tablero.leer_todo(conectar())
        except psycopg2.Error as error:
            conectar.clear()
            if intento == 2:
                st.error(f'No hay conexion con el warehouse o faltan las vistas del streaming: {error}'.strip())
                st.stop()


def _numero(valor, decimales=2, sufijo=''):
    return '—' if valor is None or valor != valor else f'{valor:,.{decimales}f}{sufijo}'


st.title('Recolección de encuestas en tiempo real')
st.caption('SIMULACIÓN: los hogares de la ECV 2025 ya cargados en el DW se re-publican por Kafka como si '
           'llegaran del trabajo de campo. Las cifras son acumuladas desde el inicio de la demo.')


@st.fragment(run_every=REFRESCO_S)
def panel():
    datos = leer()
    nacional = datos['nacional'].iloc[0]
    salud = datos['salud'].iloc[0]
    departamentos = datos['departamentos']

    # --- Indicadores principales (RS1, RS2, RS3, RS4) ---
    k1, k2, k3, k4 = st.columns(4)
    k1.metric('Hogares recibidos (RS1)', f"{int(nacional['hogares_recibidos']):,}",
              help='Hogares válidos procesados frente a la muestra total del DW')
    k1.progress(min(float(nacional['avance_pct'] or 0) / 100, 1.0),
                text=f"Avance {_numero(nacional['avance_pct'], sufijo=' %')} de {int(nacional['hogares_esperados']):,}")
    prevalencia = nacional['prevalencia_moderada_grave_pct']
    referencia = nacional['referencia_nacional_pct']
    k2.metric('Prevalencia acumulada (RS2)', _numero(prevalencia, sufijo=' %'),
              delta=None if prevalencia != prevalencia or prevalencia is None
              else f'{prevalencia - referencia:+.2f} pts frente al DW',
              delta_color='inverse',
              help=f'Inseguridad alimentaria moderada o grave, ponderada por el factor de expansión. '
                   f'Referencia del DW (DANE): {_numero(referencia, sufijo=" %")}')
    k3.metric('Departamentos en alerta (RS3)', int(nacional['departamentos_en_alerta']),
              help='Límite inferior del IC 95 % de la prevalencia acumulada por encima de la referencia nacional, '
                   'con al menos 30 hogares con FIES')
    invalidos = int(salud['mensajes'] - salud['validos'] - salud['pendientes'])
    k4.metric('Mensajes inválidos (RS4)', f'{invalidos:,} de {int(salud["mensajes"]):,}',
              help=f"Latencia media {_numero(salud['latencia_media_ms'], 1, ' ms')}, "
                   f"p95 {_numero(salud['latencia_p95_ms'], 1, ' ms')}")

    izquierda, derecha = st.columns([3, 2])

    # --- Evolucion de la prevalencia acumulada (RS2) ---
    with izquierda:
        st.subheader('Prevalencia acumulada en el tiempo')
        evolucion = datos['evolucion']
        if evolucion.empty:
            st.info('Aún no llegan encuestas. Arrancar el consumidor y el productor.')
        else:
            linea = alt.Chart(evolucion).mark_line(point=True).encode(
                x=alt.X('intervalo:T', title='Hora de procesamiento (intervalos de 10 s)'),
                y=alt.Y('prevalencia_acumulada_pct:Q', title='Prevalencia acumulada (%)', scale=alt.Scale(zero=False)),
                tooltip=['intervalo:T', 'hogares_acumulados:Q', 'prevalencia_acumulada_pct:Q'])
            regla = alt.Chart().mark_rule(strokeDash=[6, 4], color='#7f8c8d').encode(y=alt.datum(float(referencia)))
            st.altair_chart(linea + regla, use_container_width=True)
            st.caption(f'Línea punteada: referencia nacional del DW ({_numero(referencia, sufijo=" %")}). '
                       'Al reproducir todos los hogares, la curva llega exactamente a ella.')

    # --- Salud del flujo (RS4) ---
    with derecha:
        st.subheader('Estado de los mensajes')
        st.altair_chart(alt.Chart(tablero.estados_mensajes(datos['salud'])).mark_bar().encode(
            x=alt.X('mensajes:Q', title='Mensajes', axis=alt.Axis(format='d', tickMinStep=1)),
            y=alt.Y('estado:N', title=None, sort=None),
            tooltip=['estado', 'mensajes']), use_container_width=True)
        st.caption(f"Últimos 60 s: {int(salud['eventos_ultimo_minuto']):,} hogares procesados")

    # --- Departamentos (RS1-RS3) ---
    st.subheader('Prevalencia acumulada por departamento')
    con_datos = departamentos[departamentos['hogares_recibidos'] > 0]
    if not con_datos.empty:
        barras = alt.Chart(con_datos).mark_bar().encode(
            x=alt.X('nombre_departamento:N', sort='-y', title=None),
            y=alt.Y('prevalencia_moderada_grave_pct:Q', title='Prevalencia acumulada (%)'),
            color=alt.Color('estado_alerta:N', scale=COLORES_ALERTA, title='Estado'),
            tooltip=['nombre_departamento', 'hogares_recibidos', 'hogares_con_fies', 'avance_pct',
                     'prevalencia_moderada_grave_pct', 'limite_inferior_ic95_pct', 'estado_alerta'])
        st.altair_chart(barras + alt.Chart().mark_rule(strokeDash=[6, 4], color='#7f8c8d').encode(
            y=alt.datum(float(referencia))), use_container_width=True)
    st.dataframe(
        departamentos[['nombre_departamento', 'hogares_recibidos', 'hogares_esperados', 'avance_pct',
                       'prevalencia_moderada_grave_pct', 'limite_inferior_ic95_pct', 'estado_alerta']],
        hide_index=True, use_container_width=True,
        column_config={
            'nombre_departamento': 'Departamento', 'hogares_recibidos': 'Recibidos',
            'hogares_esperados': 'Esperados',
            'avance_pct': st.column_config.ProgressColumn('Avance', format='%.1f %%', min_value=0, max_value=100),
            'prevalencia_moderada_grave_pct': st.column_config.NumberColumn('Prevalencia (%)', format='%.2f'),
            'limite_inferior_ic95_pct': st.column_config.NumberColumn('Lím. inf. IC 95 % (%)', format='%.2f'),
            'estado_alerta': 'Estado',
        })
    st.caption(f'Actualizado {datetime.now():%H:%M:%S}. Se refresca cada {REFRESCO_S} s. '
               'Alerta: indicador operativo de monitoreo; no considera el efecto de diseño de la ECV.')


panel()
