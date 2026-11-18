-- =====================================================================
-- Vistas del streaming (consumidas por el dashboard en tiempo real).
-- Se recrean al final de cada carga (src/carga.py), en la misma
-- transaccion, despues de sql/vistas_analiticas.sql.
--
-- Escala: las probabilidades FIES ya vienen en 0-100, asi que las
-- prevalencias resultan en porcentaje sin multiplicar por 100.
-- Prevalencia = suma(factor x probabilidad) / suma(factor), sobre hogares
-- con FIES, igual que en las vistas R1-R7 y en la regla C11.
--
-- Metricas (README, seccion 8):
--   RS1 avance de la recoleccion por departamento
--   RS2 prevalencia acumulada por departamento y nacional
--   RS3 alerta temprana por departamento
--   RS4 salud del flujo de eventos
-- =====================================================================

-- Orden de borrado: primero las vistas que dependen de otras.
DROP VIEW IF EXISTS vw_stream_nacional;
DROP VIEW IF EXISTS vw_stream_departamento;
DROP VIEW IF EXISTS vw_stream_referencia_nacional;
DROP VIEW IF EXISTS vw_stream_evolucion;
DROP VIEW IF EXISTS vw_stream_salud_flujo;
DROP MATERIALIZED VIEW IF EXISTS mv_stream_referencia_nacional;
DROP MATERIALIZED VIEW IF EXISTS mv_stream_esperados_departamento;

-- ---------------------------------------------------------------------
-- Referencias fijas del DW: VISTAS MATERIALIZADAS.
-- La referencia nacional y los hogares esperados por departamento no
-- cambian entre cargas, pero calcularlos recorre los 87.060 hogares
-- (~160 ms). El dashboard consulta cada 2 s: con vistas normales esas
-- consultas competian con el consumidor (medido en Docker: el consumidor
-- bajaba de ~90 a ~40 eventos/s). Se calculan una vez al crearse, y este
-- archivo se ejecuta al final de cada carga (src/carga.py), con el DW ya
-- cargado: siempre corresponden a la carga vigente.
-- ---------------------------------------------------------------------

-- Referencia nacional del DW (cifra del DANE, validada por la regla C11).
-- Se calcula aqui y no se escribe fija en el codigo.
CREATE MATERIALIZED VIEW mv_stream_referencia_nacional AS
SELECT
	COUNT(*) AS hogares_esperados,
	SUM(f.factor_expansion * f.prob_ia_moderada_grave) FILTER (WHERE f.prob_ia_moderada_grave IS NOT NULL)
		/ NULLIF(SUM(f.factor_expansion) FILTER (WHERE f.prob_ia_moderada_grave IS NOT NULL), 0)
		AS prevalencia_moderada_grave_pct,
	SUM(f.factor_expansion * f.prob_ia_grave) FILTER (WHERE f.prob_ia_grave IS NOT NULL)
		/ NULLIF(SUM(f.factor_expansion) FILTER (WHERE f.prob_ia_grave IS NOT NULL), 0)
		AS prevalencia_grave_pct
FROM fact_seguridad_alimentaria_hogar f;

-- Hogares esperados por departamento (tamano de muestra de la ECV).
CREATE MATERIALIZED VIEW mv_stream_esperados_departamento AS
SELECT g.codigo_departamento, COUNT(*) AS hogares_esperados
FROM fact_seguridad_alimentaria_hogar f
JOIN dim_geografia g ON g.id_geografia = f.id_geografia
GROUP BY g.codigo_departamento;

-- Se mantiene el nombre de vista usado por el dashboard y las pruebas.
CREATE VIEW vw_stream_referencia_nacional AS
SELECT * FROM mv_stream_referencia_nacional;

-- ---------------------------------------------------------------------
-- RS1, RS2 y RS3 por departamento. Siempre 33 filas: los departamentos
-- sin eventos aparecen en 0 (LEFT JOIN desde dim_departamento).
--
-- Alerta (RS3): el limite inferior del intervalo de confianza del 95 %
-- de la prevalencia acumulada supera la prevalencia nacional del DW, con
-- al menos 30 hogares con FIES (por debajo, la aproximacion normal no es
-- fiable). Indicador operativo: ignora el efecto de diseno de la ECV.
-- ---------------------------------------------------------------------
CREATE VIEW vw_stream_departamento AS
WITH base AS (
	SELECT
		d.id_departamento,
		d.codigo_departamento,
		d.nombre_departamento,
		COALESCE(e.hogares_esperados, 0) AS hogares_esperados,
		COALESCE(m.hogares_recibidos, 0) AS hogares_recibidos,
		COALESCE(m.hogares_con_fies, 0) AS hogares_con_fies,
		COALESCE(m.hogares_grave, 0) AS hogares_grave,
		m.suma_fex_prob_mg / NULLIF(m.suma_fex_con_fies, 0) AS prevalencia_mg,
		m.suma_fex_prob_g / NULLIF(m.suma_fex_con_fies, 0) AS prevalencia_g,
		m.actualizado_en
	FROM dim_departamento d
	LEFT JOIN mv_stream_esperados_departamento e ON e.codigo_departamento = d.codigo_departamento
	LEFT JOIN fact_metrica_departamento_stream m ON m.id_departamento = d.id_departamento
),
calculo AS (
	SELECT
		b.*,
		-- limite inferior del IC 95 %, en porcentaje: p - 1,96 * sqrt(p(1-p)/n)
		100 * ((b.prevalencia_mg / 100)
			- 1.96 * SQRT((b.prevalencia_mg / 100) * (1 - b.prevalencia_mg / 100) / NULLIF(b.hogares_con_fies, 0)))
			AS limite_inferior_ic95_pct
	FROM base b
)
SELECT
	c.id_departamento,
	c.codigo_departamento,
	c.nombre_departamento,
	c.hogares_esperados,
	c.hogares_recibidos,
	ROUND((100.0 * c.hogares_recibidos / NULLIF(c.hogares_esperados, 0))::numeric, 2) AS avance_pct,
	c.hogares_con_fies,
	ROUND(c.prevalencia_mg::numeric, 2) AS prevalencia_moderada_grave_pct,
	ROUND(c.prevalencia_g::numeric, 2) AS prevalencia_grave_pct,
	ROUND((100.0 * c.hogares_grave / NULLIF(c.hogares_con_fies, 0))::numeric, 2) AS pct_hogares_severidad_grave,
	ROUND(c.limite_inferior_ic95_pct::numeric, 2) AS limite_inferior_ic95_pct,
	ROUND(r.prevalencia_moderada_grave_pct::numeric, 2) AS referencia_nacional_pct,
	CASE
		WHEN c.hogares_con_fies < 30 THEN 'sin_muestra'
		WHEN c.limite_inferior_ic95_pct > r.prevalencia_moderada_grave_pct THEN 'alerta'
		ELSE 'normal'
	END AS estado_alerta,
	c.actualizado_en
FROM calculo c
CROSS JOIN vw_stream_referencia_nacional r;

-- ---------------------------------------------------------------------
-- RS1 y RS2 a nivel nacional (una fila), sumando el snapshot.
-- Al re-publicar el hecho completo, la prevalencia acumulada debe
-- coincidir con referencia_nacional_pct (prueba de coherencia).
-- ---------------------------------------------------------------------
CREATE VIEW vw_stream_nacional AS
SELECT
	r.hogares_esperados,
	COALESCE(SUM(m.hogares_recibidos), 0) AS hogares_recibidos,
	ROUND((100.0 * COALESCE(SUM(m.hogares_recibidos), 0) / NULLIF(r.hogares_esperados, 0))::numeric, 2) AS avance_pct,
	COALESCE(SUM(m.hogares_con_fies), 0) AS hogares_con_fies,
	ROUND((SUM(m.suma_fex_prob_mg) / NULLIF(SUM(m.suma_fex_con_fies), 0))::numeric, 2) AS prevalencia_moderada_grave_pct,
	ROUND((SUM(m.suma_fex_prob_g) / NULLIF(SUM(m.suma_fex_con_fies), 0))::numeric, 2) AS prevalencia_grave_pct,
	ROUND(r.prevalencia_moderada_grave_pct::numeric, 2) AS referencia_nacional_pct,
	(SELECT COUNT(*) FROM vw_stream_departamento WHERE estado_alerta = 'alerta') AS departamentos_en_alerta,
	-- precargados = total del snapshot - llegados por Kafka (no recorre las filas de la precarga)
	COALESCE(SUM(m.hogares_recibidos), 0)
		- (SELECT COUNT(*) FROM fact_evento_encuesta WHERE origen = 'kafka') AS hogares_precargados,
	MAX(m.actualizado_en) AS actualizado_en
FROM vw_stream_referencia_nacional r
LEFT JOIN fact_metrica_departamento_stream m ON TRUE
GROUP BY r.hogares_esperados, r.prevalencia_moderada_grave_pct;

-- ---------------------------------------------------------------------
-- Evolucion en el tiempo (para graficar como cambian las metricas).
-- Intervalos de 10 segundos de procesamiento: la demo dura minutos, asi
-- que intervalos de un minuto darian muy pocos puntos.
-- Solo recorre los eventos llegados por Kafka (indice parcial). La
-- precarga entra como linea base: totales del snapshot menos lo llegado
-- por Kafka, sin recorrer sus filas.
-- ---------------------------------------------------------------------
CREATE VIEW vw_stream_evolucion AS
WITH kafka AS (
	SELECT
		date_bin('10 seconds', momento_procesado, TIMESTAMPTZ '2000-01-01') AS intervalo,
		factor_expansion, prob_ia_moderada_grave, latencia_ms
	FROM fact_evento_encuesta
	WHERE origen = 'kafka'
),
total_kafka AS (
	SELECT COUNT(*) AS hogares,
		COALESCE(SUM(factor_expansion) FILTER (WHERE prob_ia_moderada_grave IS NOT NULL), 0) AS fex,
		COALESCE(SUM(factor_expansion * prob_ia_moderada_grave), 0) AS fex_prob
	FROM kafka
),
base AS (
	SELECT
		COALESCE(SUM(m.hogares_recibidos), 0) - t.hogares AS hogares,
		COALESCE(SUM(m.suma_fex_con_fies), 0) - t.fex AS fex,
		COALESCE(SUM(m.suma_fex_prob_mg), 0) - t.fex_prob AS fex_prob
	FROM fact_metrica_departamento_stream m CROSS JOIN total_kafka t
	GROUP BY t.hogares, t.fex, t.fex_prob
),
por_intervalo AS (
	SELECT
		intervalo,
		COUNT(*) AS eventos,
		SUM(factor_expansion) FILTER (WHERE prob_ia_moderada_grave IS NOT NULL) AS suma_fex_con_fies,
		SUM(factor_expansion * prob_ia_moderada_grave) AS suma_fex_prob_mg,
		AVG(latencia_ms) AS latencia_media_ms
	FROM kafka
	GROUP BY 1
)
SELECT
	p.intervalo,
	p.eventos,
	b.hogares + SUM(p.eventos) OVER w AS hogares_acumulados,
	ROUND(((b.fex_prob + SUM(COALESCE(p.suma_fex_prob_mg, 0)) OVER w)
		/ NULLIF(b.fex + SUM(COALESCE(p.suma_fex_con_fies, 0)) OVER w, 0))::numeric, 2) AS prevalencia_acumulada_pct,
	ROUND(p.latencia_media_ms::numeric, 1) AS latencia_media_ms
FROM por_intervalo p
CROSS JOIN base b
WINDOW w AS (ORDER BY p.intervalo);

-- ---------------------------------------------------------------------
-- RS4: salud del flujo (una fila). Estados del staging y latencia del
-- consumidor. Solo mide lo que llego por Kafka: la precarga no pasa por el
-- staging y su latencia es nula.
-- ---------------------------------------------------------------------
CREATE VIEW vw_stream_salud_flujo AS
WITH estados AS (
	SELECT
		COUNT(*) AS mensajes,
		COUNT(*) FILTER (WHERE estado = 'VALIDO') AS validos,
		COUNT(*) FILTER (WHERE estado = 'INVALIDO_ESQUEMA') AS invalidos_esquema,
		COUNT(*) FILTER (WHERE estado = 'INVALIDO_VALORES') AS invalidos_valores,
		COUNT(*) FILTER (WHERE estado = 'INVALIDO_REFERENCIA') AS invalidos_referencia,
		COUNT(*) FILTER (WHERE estado = 'DUPLICADO') AS duplicados,
		COUNT(*) FILTER (WHERE estado = 'RECIBIDO') AS pendientes,
		MAX(procesado_en) AS ultimo_procesado
	FROM stg_evento_encuesta
),
latencia AS (
	SELECT
		ROUND(AVG(latencia_ms)::numeric, 1) AS latencia_media_ms,
		ROUND((PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latencia_ms))::numeric, 1) AS latencia_p95_ms,
		COUNT(*) FILTER (WHERE momento_procesado >= now() - INTERVAL '1 minute') AS eventos_ultimo_minuto
	FROM fact_evento_encuesta
	WHERE origen = 'kafka'
)
SELECT e.mensajes, e.validos, e.invalidos_esquema, e.invalidos_valores, e.invalidos_referencia, e.duplicados,
	e.pendientes, l.latencia_media_ms, l.latencia_p95_ms, l.eventos_ultimo_minuto, e.ultimo_procesado
FROM estados e CROSS JOIN latencia l;
