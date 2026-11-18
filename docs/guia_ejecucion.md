# Guía de ejecución y verificación

Cómo levantar el sistema completo desde cero, comprobar que cada parte funciona y qué evidencia capturar para la sección 11 del README. Los comandos están para **Windows (PowerShell)**; donde cambian, se indica la variante de **macOS/Linux**.

Todos los comandos se ejecutan en la **raíz del repositorio**.

---

## 0. Requisitos

| Herramienta | Versión | Nota |
|---|---|---|
| Git | cualquiera reciente | |
| Docker Desktop | con Compose v2 y contenedores Linux | Asignar **al menos 3 GB de RAM** a Docker (Settings → Resources). Airflow usa ~1,1 GB y Kafka ~0,4 GB. |
| Python | **3.12** | Para el entorno virtual (productor, consumidor, dashboard y pruebas). |
| Power BI Desktop | | Solo para el dashboard histórico ([guía](guia_dashboard.md)). |

Puertos que se usan en el equipo (solo `127.0.0.1`): **8081** Airflow, **5434** warehouse, **9092** Kafka, **8501** Streamlit.

En equipos con 8 GB de RAM conviene no tener Airflow y el streaming encendidos a la vez: primero se corre el DAG y después se detiene Airflow (paso 3.5).

---

## 1. Preparación (una sola vez)

### 1.1 Clonar y configurar

```powershell
git clone https://github.com/Sxg0673/Inseguridad-alimentaria-final.git
Set-Location Inseguridad-alimentaria-final
Copy-Item .env.example .env
```

macOS/Linux: `cd Inseguridad-alimentaria-final` y `cp .env.example .env`.

El `.env` trae valores de desarrollo local que funcionan sin cambios. Si se cambia `WAREHOUSE_PASSWORD`, cambiarla también dentro de `ETL_DATABASE_URL`.

### 1.2 Datos de la ECV

Descargar los tres CSV de la ECV 2025 siguiendo [data/raw/README.md](../data/raw/README.md) y dejarlos en `data/raw/ecv/` con estos nombres exactos:

- `Datos de la vivienda.csv`
- `Servicios del hogar.csv`
- `Condiciones de vida del hogar y tenencia de bienes.csv`

Los datos de EVA no se descargan: el DAG los extrae de la API en cada corrida.

### 1.3 Entorno virtual

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

macOS/Linux: `python3.12 -m venv .venv`, `source .venv/bin/activate` y los mismos `pip`.

Si PowerShell no deja activar el entorno: `Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass` y repetir la activación.

`requirements.txt` instala el código del proyecto en modo editable (`-e .`), así los scripts de `streaming/` funcionan desde cualquier carpeta.

### 1.4 Pruebas automáticas (sin servicios)

```powershell
python -m pytest -q
```

**Esperado:** todas pasan; las que necesitan los CSV o PostgreSQL aparecen como *skipped*.

---

## 2. Batch: Airflow, Great Expectations y Data Warehouse

### 2.1 Levantar Airflow y el warehouse

```powershell
docker compose build
docker compose up airflow-init
docker compose up -d
docker compose ps
```

**Esperado:** `airflow-apiserver`, `airflow-scheduler`, `airflow-dag-processor`, `airflow-triggerer`, `postgres` y `warehouse` en estado `healthy` (puede tardar 1-2 minutos). Kafka **no** arranca aquí: está en el perfil `streaming`.

Interfaz de Airflow: <http://localhost:8081> (usuario y clave del `.env`, por defecto `airflow` / `airflow`).

### 2.2 Corrida normal

En la interfaz: activar el DAG `inseguridad_alimentaria_etl` y dispararlo (*Trigger*). O por consola:

```powershell
docker compose exec airflow-scheduler airflow dags unpause inseguridad_alimentaria_etl
docker compose exec airflow-scheduler airflow dags trigger inseguridad_alimentaria_etl
```

**Esperado:** las 12 tareas en `success` en unos 2-3 minutos. La compuerta 3 termina `aprobada_con_advertencias` por la advertencia A3 (Guaviare con baja cobertura agrícola).

📸 **Capturar:** la vista *Graph* con las 12 tareas en verde y el log de `cargar` con los conteos.

### 2.3 Revisar el resultado

Reportes de la corrida (en el lote más reciente de `data/batches/`):

- `calidad/compuerta_1.json`, `compuerta_2.json`, `compuerta_3.json`: reglas evaluadas, estado y métricas.
- `carga/carga.json`: filas por tabla (92.496 en total, 87.060 hogares) y tablas del streaming reiniciadas.

Consultar el warehouse:

```powershell
docker compose exec warehouse psql -U warehouse -d warehouse -c "SELECT * FROM vw_stream_nacional;"
docker compose exec warehouse psql -U warehouse -d warehouse -c "SELECT count(*) FROM fact_seguridad_alimentaria_hogar;"
```

**Esperado:** 87.060 hogares; `vw_stream_nacional` con 87.060 hogares esperados, referencia nacional 21,10 y 0 recibidos.

📸 **Capturar:** los tres JSON de las compuertas (al menos sus primeras líneas) y el resultado de las consultas.

### 2.4 Falla controlada (bloqueo por calidad)

En la interfaz de Airflow: *Trigger* con configuración y el parámetro `falla_controlada` en `true`. Por consola (macOS/Linux; en PowerShell el escape de comillas cambia según la versión, por eso se recomienda la interfaz):

```bash
docker compose exec airflow-scheduler airflow dags trigger inseguridad_alimentaria_etl -c '{"falla_controlada": true}'
```

**Esperado:** `validar_eva_cruda` en `failed` por la regla crítica **C7** (llave duplicada), todo lo posterior en `upstream_failed` y **`cargar` no se ejecuta**. El warehouse queda como estaba.

📸 **Capturar:** el grafo con la tarea en rojo, el log de `validar_eva_cruda` con la línea `CRITICA C7` y el `compuerta_2.json` con `"estado": "rechazada"`.

### 2.5 Reejecución segura

Disparar otra corrida normal. **Esperado:** `success` y los mismos conteos en `carga.json` (la carga reemplaza el contenido en una transacción; no duplica).

📸 **Capturar:** los dos `carga.json` con los mismos conteos.

### 2.6 Dashboard histórico

Con el warehouse cargado se puede trabajar el dashboard en Power BI: [guía del dashboard](guia_dashboard.md).

---

## 3. Streaming: Kafka, consumidor y dashboard en tiempo real

Requisito: el DW cargado (sección 2). El streaming re-publica los hogares del hecho del DW.

### 3.1 Levantar Kafka

```powershell
docker compose --profile streaming up -d warehouse kafka kafka-init
docker compose --profile streaming logs kafka-init
```

**Esperado:** el log de `kafka-init` muestra `Topic: encuestas-hogares ... PartitionCount: 3`.

### 3.2 Pruebas del flujo contra el warehouse

Estas pruebas crean un esquema propio en la base, trabajan solo ahí y lo borran al terminar: no tocan los datos del warehouse.

```powershell
$env:ETL_DATABASE_URL = "postgresql://warehouse:warehouse@127.0.0.1:5434/warehouse"
python -m pytest -q -m warehouse tests/test_streaming_flujo.py
```

macOS/Linux: `export ETL_DATABASE_URL=postgresql://warehouse:warehouse@127.0.0.1:5434/warehouse`.

**Esperado:** todas pasan.

No correr `pytest -m integracion` contra el warehouse con datos: esas pruebas ejecutan la carga completa.

### 3.3 Demo (unos 2 minutos)

El operativo de campo ya va avanzado: el 90 % de las encuestas se precarga y el resto llega por Kafka. Abrir **tres terminales** en la raíz, con el entorno virtual activado en cada una.

```powershell
# Terminal 1: dejar la demo en cero y precargar el 90 %
python streaming/reiniciar_demo.py --si
python streaming/precargar.py --porcentaje 90
streamlit run streaming/dashboard.py

# Terminal 2: consumidor
python streaming/consumidor.py

# Terminal 3: productor (los ~8.700 hogares que faltan, con 6 eventos dañados a propósito)
python streaming/productor.py --pendientes --eventos-por-segundo 100 --con-invalidos
```

El dashboard se abre en <http://localhost:8501> y se actualiza cada 2 segundos.

**Esperado:**
- Al iniciar: 78.354 hogares recibidos (90 %), prevalencia acumulada ~21,08 %, 13 departamentos en alerta.
- Mientras corre: los hogares recibidos y el avance suben, la curva de prevalencia se mueve y aparecen los mensajes inválidos.
- Al terminar: 87.060 hogares (100 %), prevalencia acumulada **21,10 %**, igual a la referencia del DW, y **6 de 8.712** mensajes inválidos.
- El consumidor muestra 6 líneas `WARNING`: 3 `INVALIDO_ESQUEMA`, 1 `INVALIDO_VALORES`, 1 `INVALIDO_REFERENCIA` y 1 `DUPLICADO`, cada una con su motivo.

📸 **Capturar:** el dashboard al inicio, a la mitad y al final; la terminal del consumidor con los `WARNING`; la del productor con la línea `Listo`.

```powershell
docker compose exec warehouse psql -U warehouse -d warehouse -c "SELECT estado, motivo FROM stg_evento_encuesta WHERE estado <> 'VALIDO';"
```

📸 **Capturar:** el resultado (los 6 eventos rechazados con su motivo).

### 3.4 Experimento: consumidor detenido

1. Terminal 2 detenida. `python streaming/reiniciar_demo.py --si` y `python streaming/precargar.py --porcentaje 90`.
2. Terminal 2: `python streaming/consumidor.py`. Terminal 3: `python streaming/productor.py --pendientes --eventos-por-segundo 50`.
3. A los ~20 s, **Ctrl+C** en el consumidor. **Esperado:** termina limpio (`Detenido a pedido`) con el resumen de lo atendido; el dashboard deja de avanzar mientras el productor sigue publicando.
4. A los ~30 s, arrancar de nuevo el consumidor. **Esperado:** retoma desde el último mensaje confirmado y alcanza al productor.
5. Repetir cerrando la ventana del consumidor de golpe en lugar de Ctrl+C.
6. Al terminar:

```powershell
docker compose exec warehouse psql -U warehouse -d warehouse -c "SELECT count(*) AS hechos, count(DISTINCT (directorio, orden)) AS hogares FROM fact_evento_encuesta;"
```

**Esperado:** 87.060 y 87.060 (sin pérdidas ni duplicados). Si al reiniciar aparece `reentrega de un mensaje ya procesado: se omite`, es el comportamiento previsto (el lote ya estaba guardado en la base).

📸 **Capturar:** las terminales del consumidor (detención y reanudación) y la consulta final.

### 3.5 Experimento: Kafka caído

Con el consumidor corriendo: `docker compose --profile streaming stop kafka`.

**Esperado:** el consumidor termina con un error claro (`Se perdio la conexion con Kafka` o `Kafka no responde`) y código de salida 1. Después `docker compose --profile streaming start kafka`, arrancar el consumidor y comprobar que retoma.

### 3.6 Prueba de coherencia (sin precarga)

```powershell
python streaming/reiniciar_demo.py --si
python streaming/consumidor.py            # terminal 2
python streaming/productor.py --completo  # terminal 3: los 87.060 hogares sin pausa
```

**Esperado:** en menos de un par de minutos la prevalencia acumulada nacional y por departamento coincide con la del DW:

```powershell
docker compose exec warehouse psql -U warehouse -d warehouse -c "SELECT prevalencia_moderada_grave_pct, referencia_nacional_pct FROM vw_stream_nacional;"
```

### 3.7 Una carga nueva reinicia el streaming

Con el streaming lleno, volver a levantar Airflow (`docker compose up -d`) y correr el DAG. **Esperado:** la carga termina en `success` y el dashboard vuelve a 0 (las tablas del streaming tienen FK a las dimensiones de la carga vigente y se vacían con ella; ver `src/carga.py`).

---

## 4. Apagar

```powershell
docker compose --profile streaming stop
```

Los datos se conservan en los volúmenes de Docker. **No** usar `docker compose down -v`: borra los volúmenes (warehouse y Kafka).

Para liberar RAM entre la parte batch y el streaming:

```powershell
docker compose stop airflow-apiserver airflow-scheduler airflow-dag-processor airflow-triggerer postgres
```

---

## 5. Problemas frecuentes

| Síntoma | Causa y solución |
|---|---|
| `No hay conexion con el warehouse` | El servicio `warehouse` no está arriba o `ETL_DATABASE_URL` en `.env` no apunta a `127.0.0.1:5434`. |
| `No hay conexion con Kafka` / `El topic ... no existe` | Falta `docker compose --profile streaming up -d warehouse kafka kafka-init`. |
| `El hecho de hogares esta vacio` | Falta correr el DAG (sección 2). |
| `El streaming ya tiene datos` al precargar | Correr antes `python streaming/reiniciar_demo.py --si`. |
| `reiniciar_demo` dice que hay consumidores activos | Detener el consumidor (Ctrl+C) y repetir. |
| El productor avisa que los hogares quedarán como `DUPLICADO` | Ya hay datos de una demo anterior: reiniciar la demo. |
| Kafka se reinicia en bucle después de un apagado brusco | Un archivo de control del broker quedó dañado. Como el streaming se puede regenerar, la solución simple es detener el perfil y recrear el volumen de Kafka: `docker compose --profile streaming rm -sf kafka` y `docker volume rm inseguridad-alimentaria-final_kafka-data`; después `python streaming/reiniciar_demo.py --si`. |
| Las tareas de Airflow quedan en *queued* | Poca RAM para Docker: subirla a 3 GB o más y cerrar aplicaciones pesadas. |
