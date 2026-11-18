# Evidencia de la corrida `corrida_3_falla_controlada`

- Lote: `e14692d26a833bcd23a3ecd7` (creado 2026-11-18T01:41:22+00:00)
- Resultado: detenida antes de la carga

## Compuertas de calidad (Great Expectations)

| Compuerta | Nombre | Estado | Reglas evaluadas | Críticas fallidas | Advertencias |
|---:|---|---|---:|---|---|
| 1 | ECV cruda | aprobada | 2 | - | - |
| 2 | EVA cruda (API) | rechazada | 9 | C7 | - |

1 compuerta no se ejecutó: la corrida se detuvo antes.

### Expectativas no cumplidas

- Compuerta 2, **C7** (crítica): Llave municipio + codigo de cultivo + periodo unica — tabla `eva_registros`, 2 valores inesperados (ejemplos: `{'c_digo_dane_municipio': '05001', 'c_digo_del_cultivo': '1050600', 'periodo': '2024A'}`, `{'c_digo_dane_municipio': '05001', 'c_digo_del_cultivo': '1050600', 'periodo': '2024A'}`)

### Métricas de conciliación

| Métrica | Valor |
|---|---:|
| diferencia_extraidos_api_2024 | 0 |
| departamentos_2024 | 32 |
| diferencia_extraidos_api_2025 | 0 |
| departamentos_2025 | 32 |

Generado con `python scripts/exportar_evidencia.py`. Los JSON de esta carpeta son copias exactas (verificadas por sha256) de los del lote.
