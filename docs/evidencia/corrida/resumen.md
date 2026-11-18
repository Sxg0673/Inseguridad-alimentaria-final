# Evidencia de la corrida `scheduled__2026-11-15T00:00:00+00:00`

- Lote: `90db259ea0db8b24b865d6d8` (creado 2026-11-18T03:05:25+00:00)
- Resultado: cargada en el warehouse

## Compuertas de calidad (Great Expectations)

| Compuerta | Nombre | Estado | Reglas evaluadas | Críticas fallidas | Advertencias |
|---:|---|---|---:|---|---|
| 1 | ECV cruda | aprobada | 2 | - | - |
| 2 | EVA cruda (API) | aprobada | 9 | - | - |
| 3 | Datos preparados | aprobada_con_advertencias | 8 | - | A3 |

### Expectativas no cumplidas

- Compuerta 3, **A3** (advertencia): Departamento con menos de 15 cultivos comparables (baja cobertura) — tabla `fact_indicadores_departamento`, 1 valor inesperado (ejemplos: `True`)

### Métricas de conciliación

| Métrica | Valor |
|---|---:|
| diferencia_extraidos_api_2024 | 0 |
| departamentos_2024 | 32 |
| diferencia_extraidos_api_2025 | 0 |
| departamentos_2025 | 32 |
| diferencia_filas_hecho | 0,0000 |
| prevalencia_nacional_hecho | 21,0956 |
| prevalencia_recompuesta_departamentos | 21,0972 |
| diferencia_hogares_departamentos | 0,0000 |
| diferencia_produccion_t | 0,0000 |

## Carga en el warehouse

- Estrategia: TRUNCATE + INSERT en una transaccion
- Estado de la compuerta 3 al cargar: aprobada_con_advertencias

| Tabla | Filas |
|---|---:|
| dim_geografia | 65 |
| dim_nivel_ingreso | 6 |
| dim_nivel_severidad_ia | 5 |
| dim_choque_economico | 172 |
| dim_estrategia_afrontamiento | 265 |
| dim_departamento | 33 |
| dim_cultivo | 160 |
| fact_seguridad_alimentaria_hogar | 87.060 |
| fact_produccion_agricola | 3.204 |
| fact_variacion_produccion | 1.493 |
| fact_indicadores_departamento | 33 |
| **Total** | **92.496** |

Tablas del streaming reiniciadas por la carga: stg_evento_encuesta, fact_evento_encuesta, fact_metrica_departamento_stream.

Generado con `python scripts/exportar_evidencia.py`. Los JSON de esta carpeta son copias exactas (verificadas por sha256) de los del lote.
