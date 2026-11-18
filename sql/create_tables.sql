-- =====================================================================
-- DDL del Data Warehouse - Entrega final
-- Motor: PostgreSQL 16. Esquema: public.
-- Modelo: docs/modelo_dimensional.dbml
--
-- Re-ejecucion segura: todas las tablas se crean con IF NOT EXISTS y el
-- script no borra nada, para que correrlo de nuevo no destruya datos.
-- Que una carga repetida no duplique filas lo garantiza la carga, no el DDL.
--
-- Orden: dimensiones antes que hechos, porque los hechos las referencian.
--
-- Secciones: hogares (Primera Entrega), agricola (Segunda Entrega) y
-- streaming (Entrega final).
-- =====================================================================


-- =====================================================================
-- HOGARES (Primera Entrega, mismas columnas y restricciones)
-- =====================================================================

-- ---------------------------------------------------------------------
-- DIM_GEOGRAFIA
-- Grano: una fila por combinacion de departamento y clase de area.
-- La region depende de ambas, no solo del departamento (caso Bogota).
-- codigo_departamento es el atributo conformado con dim_departamento.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_geografia (
    id_geografia            INTEGER NOT NULL,
    codigo_departamento     VARCHAR(2) NOT NULL,
    nombre_departamento     TEXT NOT NULL,
    codigo_region           VARCHAR(1) NOT NULL,
    nombre_region           TEXT NOT NULL,
    codigo_clase            VARCHAR(1) NOT NULL,
    nombre_clase            TEXT NOT NULL,
    CONSTRAINT pk_dim_geografia PRIMARY KEY (id_geografia)
);

-- ---------------------------------------------------------------------
-- DIM_NIVEL_INGRESO
-- Grano: una fila por quintil de ingreso per capita del hogar, mas la
-- categoria "Dato inconsistente".
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_nivel_ingreso (
    id_nivel_ingreso        INTEGER NOT NULL,
    codigo_quintil          VARCHAR(20) NOT NULL,
    nombre_quintil          TEXT NOT NULL,
    CONSTRAINT pk_dim_nivel_ingreso PRIMARY KEY (id_nivel_ingreso)
);

-- ---------------------------------------------------------------------
-- DIM_NIVEL_SEVERIDAD_IA
-- Grano: una fila por nivel de severidad FIES (puntaje crudo 0 a 8).
-- "Sin dato" no tiene rango numerico definido.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_nivel_severidad_ia (
    id_nivel_severidad_ia   INTEGER NOT NULL,
    nombre_severidad        TEXT NOT NULL,
    puntaje_fies_minimo     INTEGER,
    puntaje_fies_maximo     INTEGER,
    CONSTRAINT pk_dim_nivel_severidad_ia PRIMARY KEY (id_nivel_severidad_ia)
);

-- ---------------------------------------------------------------------
-- DIM_CHOQUE_ECONOMICO (junk dimension)
-- Grano: una fila por combinacion de choques realmente observada.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_choque_economico (
    id_choque_economico                    INTEGER NOT NULL,
    choque_jefe_perdio_empleo              BOOLEAN NOT NULL,
    choque_conyuge_perdio_empleo           BOOLEAN NOT NULL,
    choque_otro_miembro_perdio_empleo      BOOLEAN NOT NULL,
    choque_cierre_negocio                  BOOLEAN NOT NULL,
    choque_no_vendio_produccion            BOOLEAN NOT NULL,
    choque_atraso_jardin_colegio           BOOLEAN NOT NULL,
    choque_no_pago_universidad             BOOLEAN NOT NULL,
    choque_atraso_vivienda                 BOOLEAN NOT NULL,
    choque_atraso_administracion           BOOLEAN NOT NULL,
    choque_atraso_servicios_publicos       BOOLEAN NOT NULL,
    choque_atraso_impuestos                BOOLEAN NOT NULL,
    numero_choques                         INTEGER NOT NULL,
    CONSTRAINT pk_dim_choque_economico PRIMARY KEY (id_choque_economico)
);

-- ---------------------------------------------------------------------
-- DIM_ESTRATEGIA_AFRONTAMIENTO (junk dimension)
-- Grano: una fila por combinacion de estrategias realmente observada.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_estrategia_afrontamiento (
    id_estrategia_afrontamiento            INTEGER NOT NULL,
    estrategia_empezaron_a_trabajar        BOOLEAN NOT NULL,
    estrategia_nuevas_fuentes_ingreso      BOOLEAN NOT NULL,
    estrategia_vivir_con_familiares        BOOLEAN NOT NULL,
    estrategia_gastaron_ahorros            BOOLEAN NOT NULL,
    estrategia_se_endeudaron               BOOLEAN NOT NULL,
    estrategia_vendieron_bienes            BOOLEAN NOT NULL,
    estrategia_vendieron_vivienda          BOOLEAN NOT NULL,
    estrategia_retiro_escolar              BOOLEAN NOT NULL,
    estrategia_retiro_universidad          BOOLEAN NOT NULL,
    estrategia_disminuyeron_gasto_alimentos BOOLEAN NOT NULL,
    estrategia_ayuda_gobierno              BOOLEAN NOT NULL,
    estrategia_ayuda_familiares_amigos     BOOLEAN NOT NULL,
    estrategia_subsidio_desempleo          BOOLEAN NOT NULL,
    estrategia_otra                        BOOLEAN NOT NULL,
    numero_estrategias                     INTEGER NOT NULL,
    CONSTRAINT pk_dim_estrategia_afrontamiento PRIMARY KEY (id_estrategia_afrontamiento)
);

-- ---------------------------------------------------------------------
-- FACT_SEGURIDAD_ALIMENTARIA_HOGAR
-- Grano: un hogar encuestado en la ECV 2025. R1-R5.
-- directorio y orden: llave natural del DANE, para trazabilidad.
-- prob_ia_* aceptan nulo: 718 hogares sin FIES valido.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_seguridad_alimentaria_hogar (
    id_hogar                        INTEGER NOT NULL,
    directorio                      VARCHAR(10) NOT NULL,
    orden                           VARCHAR(2) NOT NULL,
    id_geografia                    INTEGER NOT NULL,
    id_nivel_ingreso                INTEGER NOT NULL,
    id_nivel_severidad_ia           INTEGER NOT NULL,
    id_choque_economico             INTEGER NOT NULL,
    id_estrategia_afrontamiento     INTEGER NOT NULL,
    tiene_acueducto                 BOOLEAN NOT NULL,
    tiene_alcantarillado            BOOLEAN NOT NULL,
    factor_expansion                DECIMAL(15, 9) NOT NULL,
    prob_ia_moderada_grave          DECIMAL(12, 9),
    prob_ia_grave                   DECIMAL(12, 9),
    CONSTRAINT pk_fact_seguridad_alimentaria_hogar PRIMARY KEY (id_hogar),
    CONSTRAINT uq_fact_directorio_orden UNIQUE (directorio, orden),
    CONSTRAINT fk_hecho_geografia
        FOREIGN KEY (id_geografia) REFERENCES dim_geografia (id_geografia),
    CONSTRAINT fk_hecho_nivel_ingreso
        FOREIGN KEY (id_nivel_ingreso) REFERENCES dim_nivel_ingreso (id_nivel_ingreso),
    CONSTRAINT fk_hecho_nivel_severidad_ia
        FOREIGN KEY (id_nivel_severidad_ia) REFERENCES dim_nivel_severidad_ia (id_nivel_severidad_ia),
    CONSTRAINT fk_hecho_choque_economico
        FOREIGN KEY (id_choque_economico) REFERENCES dim_choque_economico (id_choque_economico),
    CONSTRAINT fk_hecho_estrategia_afrontamiento
        FOREIGN KEY (id_estrategia_afrontamiento) REFERENCES dim_estrategia_afrontamiento (id_estrategia_afrontamiento)
);


-- =====================================================================
-- AGRICOLA (Segunda Entrega)
-- =====================================================================

-- ---------------------------------------------------------------------
-- DIM_DEPARTAMENTO
-- Dimension conformada reducida de dim_geografia: mismos codigos y
-- nombres DIVIPOLA, solo nivel departamento (33 filas, Bogota incluida).
-- No tiene FK con dim_geografia: el vinculo es logico por
-- codigo_departamento y se valida al cargar.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_departamento (
    id_departamento         INTEGER NOT NULL,
    codigo_departamento     VARCHAR(2) NOT NULL,
    nombre_departamento     TEXT NOT NULL,
    CONSTRAINT pk_dim_departamento PRIMARY KEY (id_departamento),
    CONSTRAINT uq_dim_departamento_codigo UNIQUE (codigo_departamento)
);

-- ---------------------------------------------------------------------
-- DIM_CULTIVO
-- Grano: un cultivo de EVA (160 filas; 139 en la canasta).
-- en_canasta y motivo_canasta vienen de config/canasta_cultivos.csv.
-- Ciclo, estado fisico, grupo y subgrupo son unicos por cultivo
-- (verificado sobre los 51.170 registros de 2024-2025).
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_cultivo (
    id_cultivo              INTEGER NOT NULL,
    nombre_cultivo          TEXT NOT NULL,
    grupo_cultivo           TEXT NOT NULL,
    subgrupo                TEXT NOT NULL,
    ciclo_cultivo           TEXT NOT NULL,
    estado_fisico           TEXT NOT NULL,
    en_canasta              BOOLEAN NOT NULL,
    motivo_canasta          TEXT NOT NULL,
    CONSTRAINT pk_dim_cultivo PRIMARY KEY (id_cultivo),
    CONSTRAINT uq_dim_cultivo_nombre UNIQUE (nombre_cultivo)
);

-- ---------------------------------------------------------------------
-- FACT_PRODUCCION_AGRICOLA
-- Grano: departamento x cultivo x anio. Suma municipios y periodos de
-- EVA. anio es dimension degenerada (2024 o 2025). Incluye los 160
-- cultivos; la canasta se filtra con dim_cultivo.en_canasta.
-- fecha_extraccion: la UPRA puede revisar las cifras de 2024 y 2025.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_produccion_agricola (
    id_produccion           INTEGER NOT NULL,
    id_departamento         INTEGER NOT NULL,
    id_cultivo              INTEGER NOT NULL,
    anio                    SMALLINT NOT NULL,
    area_sembrada_ha        DECIMAL(14, 2) NOT NULL,
    area_cosechada_ha       DECIMAL(14, 2) NOT NULL,
    produccion_t            DECIMAL(16, 2) NOT NULL,
    numero_municipios       INTEGER NOT NULL,
    fecha_extraccion        TIMESTAMPTZ NOT NULL,
    CONSTRAINT pk_fact_produccion_agricola PRIMARY KEY (id_produccion),
    CONSTRAINT uq_fact_produccion_grano UNIQUE (id_departamento, id_cultivo, anio),
    CONSTRAINT ck_fact_produccion_anio CHECK (anio IN (2024, 2025)),
    CONSTRAINT ck_fact_produccion_no_negativos CHECK (
        area_sembrada_ha >= 0 AND area_cosechada_ha >= 0 AND produccion_t >= 0
    ),
    CONSTRAINT fk_produccion_departamento
        FOREIGN KEY (id_departamento) REFERENCES dim_departamento (id_departamento),
    CONSTRAINT fk_produccion_cultivo
        FOREIGN KEY (id_cultivo) REFERENCES dim_cultivo (id_cultivo)
);

-- ---------------------------------------------------------------------
-- FACT_VARIACION_PRODUCCION (hecho consolidado, R7 por cultivo)
-- Grano: departamento x cultivo, comparacion 2024 -> 2025.
-- Solo cultivos de la canasta. Las producciones son nulas si el cultivo
-- no tiene registro ese anio; variacion_pct y en_descenso son nulos si
-- el par no es comparable.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_variacion_produccion (
    id_departamento         INTEGER NOT NULL,
    id_cultivo              INTEGER NOT NULL,
    produccion_2024_t       DECIMAL(16, 2),
    produccion_2025_t       DECIMAL(16, 2),
    variacion_pct           DECIMAL(10, 2),
    estado_comparabilidad   VARCHAR(20) NOT NULL,
    en_descenso             BOOLEAN,
    CONSTRAINT pk_fact_variacion_produccion PRIMARY KEY (id_departamento, id_cultivo),
    CONSTRAINT ck_fact_variacion_estado CHECK (
        estado_comparabilidad IN ('comparable', 'base_cero', 'solo_2024', 'solo_2025')
    ),
    CONSTRAINT ck_fact_variacion_no_negativos CHECK (
        produccion_2024_t >= 0 AND produccion_2025_t >= 0
    ),
    CONSTRAINT fk_variacion_departamento
        FOREIGN KEY (id_departamento) REFERENCES dim_departamento (id_departamento),
    CONSTRAINT fk_variacion_cultivo
        FOREIGN KEY (id_cultivo) REFERENCES dim_cultivo (id_cultivo)
);

-- ---------------------------------------------------------------------
-- FACT_INDICADORES_DEPARTAMENTO (hecho consolidado, R6 y R7)
-- Grano: departamento, 2025 (33 filas). Une ECV y EVA (drill-across).
-- Los indicadores agricolas son nulos para Bogota (sin dato en EVA),
-- nunca 0. baja_cobertura: menos de 15 cultivos comparables (regla A3).
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_indicadores_departamento (
    id_departamento                 INTEGER NOT NULL,
    hogares_muestra                 INTEGER NOT NULL,
    hogares_con_fies                INTEGER NOT NULL,
    hogares_expandidos              DECIMAL(16, 2) NOT NULL,
    prevalencia_ia_moderada_grave   DECIMAL(6, 2) NOT NULL,
    prevalencia_ia_grave            DECIMAL(6, 2) NOT NULL,
    diversidad_cultivos_2025        INTEGER,
    cultivos_comparables            INTEGER,
    cultivos_en_descenso            INTEGER,
    pct_cultivos_en_descenso        DECIMAL(6, 2),
    baja_cobertura                  BOOLEAN,
    CONSTRAINT pk_fact_indicadores_departamento PRIMARY KEY (id_departamento),
    CONSTRAINT fk_indicadores_departamento
        FOREIGN KEY (id_departamento) REFERENCES dim_departamento (id_departamento)
);


-- =====================================================================
-- STREAMING (Entrega final)
-- Llegada simulada de encuestas de hogar por Kafka (re-publicacion de
-- fact_seguridad_alimentaria_hogar). Documentado en la
-- seccion 8 del README.
--
-- Estas tablas se vacian en el mismo TRUNCATE de la carga (src/carga.py):
-- dependen de las claves sustitutas de la carga vigente, asi que una
-- nueva carga del DW reinicia el streaming. No se cargan desde el lote:
-- las llena el consumidor de Kafka.
-- =====================================================================

-- ---------------------------------------------------------------------
-- STG_EVENTO_ENCUESTA (staging, no es parte del modelo analitico)
-- Grano: un mensaje de Kafka tal como llego, guardado ANTES de validarlo.
-- (topic, particion, desplazamiento) identifica el mensaje: si Kafka lo
-- reentrega tras un reinicio, no se duplica. contenido es TEXT y no JSONB
-- para conservar tambien un JSON mal formado.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg_evento_encuesta (
    id_evento_crudo         BIGSERIAL,
    topic                   TEXT NOT NULL,
    particion               INTEGER NOT NULL,
    desplazamiento          BIGINT NOT NULL,
    clave                   TEXT,
    contenido               TEXT,
    estado                  TEXT NOT NULL DEFAULT 'RECIBIDO',
    motivo                  TEXT,
    recibido_en             TIMESTAMPTZ NOT NULL DEFAULT now(),
    procesado_en            TIMESTAMPTZ,
    CONSTRAINT pk_stg_evento_encuesta PRIMARY KEY (id_evento_crudo),
    CONSTRAINT uq_stg_evento_mensaje UNIQUE (topic, particion, desplazamiento),
    CONSTRAINT ck_stg_evento_estado CHECK (estado IN (
        'RECIBIDO', 'VALIDO', 'INVALIDO_ESQUEMA', 'INVALIDO_VALORES',
        'INVALIDO_REFERENCIA', 'DUPLICADO'))
);

-- ---------------------------------------------------------------------
-- FACT_EVENTO_ENCUESTA (hecho transaccional del streaming)
-- Grano: la llegada valida de la encuesta de un hogar, procesada por el
-- consumidor. Un hogar se encuesta una sola vez: (directorio, orden) es
-- unico; una segunda llegada queda como DUPLICADO en el staging.
-- Sin dimension de tiempo: las metricas se miden en segundos y minutos,
-- sin jerarquia de calendario; el tiempo va como atributo del hecho.
-- Probabilidades nulas si y solo si la severidad es "Sin dato" (C12).
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_evento_encuesta (
    id_evento_encuesta      BIGSERIAL,
    id_evento_crudo         BIGINT NOT NULL,
    id_evento               UUID NOT NULL,
    directorio              VARCHAR(10) NOT NULL,
    orden                   VARCHAR(2) NOT NULL,
    id_geografia            INTEGER NOT NULL,
    id_nivel_severidad_ia   INTEGER NOT NULL,
    factor_expansion        DECIMAL(15, 9) NOT NULL,
    prob_ia_moderada_grave  DECIMAL(12, 9),
    prob_ia_grave           DECIMAL(12, 9),
    momento_evento          TIMESTAMPTZ NOT NULL,
    momento_procesado       TIMESTAMPTZ NOT NULL,
    latencia_ms             INTEGER NOT NULL,
    CONSTRAINT pk_fact_evento_encuesta PRIMARY KEY (id_evento_encuesta),
    CONSTRAINT uq_evento_crudo UNIQUE (id_evento_crudo),
    CONSTRAINT uq_evento_id UNIQUE (id_evento),
    CONSTRAINT uq_evento_hogar UNIQUE (directorio, orden),
    CONSTRAINT fk_evento_crudo
        FOREIGN KEY (id_evento_crudo) REFERENCES stg_evento_encuesta (id_evento_crudo),
    CONSTRAINT fk_evento_geografia
        FOREIGN KEY (id_geografia) REFERENCES dim_geografia (id_geografia),
    CONSTRAINT fk_evento_severidad
        FOREIGN KEY (id_nivel_severidad_ia) REFERENCES dim_nivel_severidad_ia (id_nivel_severidad_ia)
);

-- ---------------------------------------------------------------------
-- FACT_METRICA_DEPARTAMENTO_STREAM (snapshot acumulado)
-- Grano: el estado acumulado del operativo de recoleccion en un
-- departamento. El consumidor la actualiza (upsert) en la misma
-- transaccion en que inserta el evento en fact_evento_encuesta.
-- Solo guarda conteos y sumas aditivas: las prevalencias se calculan en
-- vistas como cociente de sumas, nunca promediando porcentajes.
-- hogares_grave: hogares con severidad "Inseguridad grave" (puntaje FIES).
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_metrica_departamento_stream (
    id_departamento         INTEGER NOT NULL,
    hogares_recibidos       INTEGER NOT NULL DEFAULT 0,
    hogares_con_fies        INTEGER NOT NULL DEFAULT 0,
    hogares_grave           INTEGER NOT NULL DEFAULT 0,
    suma_fex_con_fies       DECIMAL(20, 9) NOT NULL DEFAULT 0,
    suma_fex_prob_mg        DECIMAL(24, 9) NOT NULL DEFAULT 0,
    suma_fex_prob_g         DECIMAL(24, 9) NOT NULL DEFAULT 0,
    actualizado_en          TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT pk_fact_metrica_departamento_stream PRIMARY KEY (id_departamento),
    CONSTRAINT fk_metrica_departamento
        FOREIGN KEY (id_departamento) REFERENCES dim_departamento (id_departamento)
);
