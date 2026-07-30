/* ============================================================================================
   02 · DDL — CREACIÓN DE TABLAS
   Motor: Impala (Hue / Cloudera Data Explorer / impala-shell)
   ============================================================================================

   ⚠ ANTES DE ELEGIR EL MODO DE ESCRITURA, LEER docs/01-decision-mor-cow.md

   Impala soporta ÚNICAMENTE merge-on-read para escritura. Una tabla configurada en
   copy-on-write hará FALLAR toda operación de DELETE/UPDATE/MERGE ejecutada desde Impala.
   Impala sí puede LEER tablas copy-on-write escritas por Hive o Spark.

   SINTAXIS POR MOTOR:
     Impala : STORED AS ICEBERG
     Hive   : STORED BY ICEBERG

   PARTICIONAMIENTO:
     PARTITIONED BY SPEC (<transforms>)   → particionamiento oculto Iceberg v2 (recomendado)
     PARTITIONED BY (<col> <tipo>)        → identity partitioning, compatible con Iceberg v1
   ============================================================================================ */


/* --------------------------------------------------------------------------------------------
   5.1 — CDR DE VOZ · MoR · MANAGED · Alta frecuencia de ingesta
   --------------------------------------------------------------------------------------------
   Patrón: CDE escribe en madrugada, CDW lee todo el día, reprocesos ocasionales por fecha.
   -------------------------------------------------------------------------------------------- */

CREATE TABLE IF NOT EXISTS telco_core.cdr_voz (
    id_cdr                BIGINT,
    msisdn_origen         STRING,
    msisdn_destino        STRING,
    imsi                  STRING,
    id_celda_origen       STRING,
    id_celda_destino      STRING,
    ts_inicio             TIMESTAMP,
    ts_fin                TIMESTAMP,
    duracion_seg          INT,
    tipo_llamada          STRING,
    id_plan               INT,
    costo                 DECIMAL(18,6),
    id_operador_destino   STRING,
    codigo_terminacion    STRING,
    fecha_evento          DATE
)
PARTITIONED BY SPEC (DAYS(fecha_evento))
STORED AS ICEBERG
TBLPROPERTIES (
    'format-version'                            = '2',

    -- Modo de escritura: MoR obligatorio (Impala ejecuta reprocesos)
    'write.delete.mode'                         = 'merge-on-read',
    'write.update.mode'                         = 'merge-on-read',
    'write.merge.mode'                          = 'merge-on-read',

    -- Formato y compresión
    'write.format.default'                      = 'parquet',
    'write.parquet.compression-codec'           = 'zstd',
    'write.target-file-size-bytes'              = '268435456',   -- 256 MB

    -- Retención de snapshots: 7 días de time travel
    'history.expire.max-snapshot-age-ms'        = '604800000',
    'history.expire.min-snapshots-to-keep'      = '400',         -- ~1 noche de ingesta CDE

    -- Limpieza de metadata (sin esto el prefijo /metadata/ crece indefinidamente)
    'write.metadata.delete-after-commit.enabled'= 'true',
    'write.metadata.previous-versions-max'      = '10',

    -- Habilita COMPUTE STATS ... TABLESAMPLE (tabla de miles de millones de registros)
    'impala.enable.stats.extrapolation'         = 'true'
);


/* --------------------------------------------------------------------------------------------
   5.2 — CDR DE DATOS · MoR · MANAGED · Partición compuesta
   --------------------------------------------------------------------------------------------
   Partición por día + bucket de MSISDN: distribuye el volumen dentro de cada día y mejora
   el pruning en consultas filtradas por suscriptor.
   -------------------------------------------------------------------------------------------- */

CREATE TABLE IF NOT EXISTS telco_core.cdr_datos (
    id_sesion             BIGINT,
    msisdn                STRING,
    imsi                  STRING,
    imei                  STRING,
    id_celda              STRING,
    apn                   STRING,
    ts_inicio             TIMESTAMP,
    ts_fin                TIMESTAMP,
    bytes_subida          BIGINT,
    bytes_bajada          BIGINT,
    tecnologia_red        STRING,          -- 3G / 4G / 5G
    id_plan               INT,
    costo                 DECIMAL(18,6),
    fecha_evento          DATE
)
PARTITIONED BY SPEC (DAYS(fecha_evento), BUCKET(16, msisdn))
STORED AS ICEBERG
TBLPROPERTIES (
    'format-version'                            = '2',
    'write.delete.mode'                         = 'merge-on-read',
    'write.update.mode'                         = 'merge-on-read',
    'write.merge.mode'                          = 'merge-on-read',
    'write.format.default'                      = 'parquet',
    'write.parquet.compression-codec'           = 'zstd',
    'write.target-file-size-bytes'              = '268435456',
    'history.expire.max-snapshot-age-ms'        = '604800000',
    'history.expire.min-snapshots-to-keep'      = '400',
    'write.metadata.delete-after-commit.enabled'= 'true',
    'write.metadata.previous-versions-max'      = '10',
    'impala.enable.stats.extrapolation'         = 'true'
);


/* --------------------------------------------------------------------------------------------
   5.3 — DIMENSIÓN DE SUSCRIPTOR · CoW · MANAGED
   --------------------------------------------------------------------------------------------
   ⚠ ADVERTENCIA CRÍTICA
   Esta tabla usa copy-on-write. TODA operación de DELETE/UPDATE/MERGE sobre ella DEBE
   ejecutarse desde Hive o Spark/CDE. Un DELETE o UPDATE desde Impala FALLARÁ.
   Impala puede LEER esta tabla sin restricción.

   Justificación de CoW: updates masivos diarios (SCD tipo 2) + lectura constante en joins.
   En MoR esta tabla acumularía delete files en cada carga, penalizando todos los joins.
   -------------------------------------------------------------------------------------------- */

CREATE TABLE IF NOT EXISTS telco_core.dim_suscriptor (
    id_suscriptor         BIGINT,
    msisdn                STRING,
    imsi                  STRING,
    documento_id          STRING,
    nombre_cliente        STRING,
    segmento              STRING,          -- Prepago / Pospago / Corporativo
    id_plan               INT,
    nombre_plan           STRING,
    estado_linea          STRING,          -- Activa / Suspendida / Baja
    fecha_activacion      DATE,
    fecha_inicio_vig      DATE,
    fecha_fin_vig         DATE,
    registro_vigente      BOOLEAN,
    fecha_carga           DATE
)
PARTITIONED BY SPEC (MONTHS(fecha_carga))
STORED AS ICEBERG
TBLPROPERTIES (
    'format-version'                            = '2',

    -- CoW: solo escribible desde Hive o Spark/CDE
    'write.delete.mode'                         = 'copy-on-write',
    'write.update.mode'                         = 'copy-on-write',
    'write.merge.mode'                          = 'copy-on-write',

    'write.format.default'                      = 'parquet',
    'write.parquet.compression-codec'           = 'zstd',
    'write.target-file-size-bytes'              = '134217728',   -- 128 MB (tabla menor)

    'history.expire.max-snapshot-age-ms'        = '2592000000',  -- 30 días (dimensión crítica)
    'history.expire.min-snapshots-to-keep'      = '30',          -- ~1 carga diaria x 30 días

    'write.metadata.delete-after-commit.enabled'= 'true',
    'write.metadata.previous-versions-max'      = '10'
);


/* --------------------------------------------------------------------------------------------
   5.4 — MODO MIXTO · Upserts masivos + borrados puntuales
   --------------------------------------------------------------------------------------------
   Se pueden combinar modos por tipo de operación. Este patrón NO es compatible con
   escritura desde Impala (write.merge.mode = CoW lo impide para MERGE).
   Válido cuando todas las escrituras corren en Spark/CDE.
   -------------------------------------------------------------------------------------------- */

CREATE TABLE IF NOT EXISTS telco_core.agregados_facturacion (
    id_ciclo              INT,
    id_suscriptor         BIGINT,
    msisdn                STRING,
    minutos_voz           BIGINT,
    sms_enviados          BIGINT,
    mb_consumidos         DECIMAL(18,4),
    cargo_fijo            DECIMAL(18,4),
    cargo_variable        DECIMAL(18,4),
    total_facturado       DECIMAL(18,4),
    estado_ciclo          STRING,
    fecha_corte           DATE
)
PARTITIONED BY SPEC (MONTHS(fecha_corte))
STORED AS ICEBERG
TBLPROPERTIES (
    'format-version'                            = '2',

    'write.delete.mode'                         = 'merge-on-read',  -- borrados puntuales rápidos
    'write.update.mode'                         = 'merge-on-read',
    'write.merge.mode'                          = 'copy-on-write',  -- upsert masivo sin delete files

    'write.format.default'                      = 'parquet',
    'write.parquet.compression-codec'           = 'zstd',
    'write.target-file-size-bytes'              = '268435456',
    'history.expire.max-snapshot-age-ms'        = '604800000',
    'history.expire.min-snapshots-to-keep'      = '20',
    'write.metadata.delete-after-commit.enabled'= 'true',
    'write.metadata.previous-versions-max'      = '10'
);


/* --------------------------------------------------------------------------------------------
   5.5 — TABLA EXTERNA · Datos compartidos con consumidores fuera de CDW
   --------------------------------------------------------------------------------------------
   ⚠ external.table.purge='true' es OBLIGATORIO si se espera que el mantenimiento libere
     espacio en S3. Sin esta propiedad, EXPIRE_SNAPSHOTS y REMOVE_ORPHAN_FILES limpian el
     catálogo pero dejan todos los parquets en el bucket.
   -------------------------------------------------------------------------------------------- */

CREATE EXTERNAL TABLE IF NOT EXISTS telco_core.eventos_red_kpi (
    id_evento             BIGINT,
    id_celda              STRING,
    id_sector             STRING,
    tecnologia_red        STRING,
    tipo_evento           STRING,
    severidad             STRING,
    valor_kpi             DOUBLE,
    umbral_kpi            DOUBLE,
    ts_evento             TIMESTAMP,
    fecha_evento          DATE
)
PARTITIONED BY SPEC (DAYS(fecha_evento))
STORED AS ICEBERG
LOCATION 's3a://<bucket-datalake>/warehouse/telco_core/eventos_red_kpi'
TBLPROPERTIES (
    'format-version'                            = '2',

    -- ⚠ SIN ESTA PROPIEDAD LOS ARCHIVOS NO SE BORRAN DE S3
    'external.table.purge'                      = 'true',

    'write.delete.mode'                         = 'merge-on-read',
    'write.update.mode'                         = 'merge-on-read',
    'write.merge.mode'                          = 'merge-on-read',
    'write.format.default'                      = 'parquet',
    'write.parquet.compression-codec'           = 'zstd',
    'write.target-file-size-bytes'              = '268435456',
    'history.expire.max-snapshot-age-ms'        = '604800000',
    'history.expire.min-snapshots-to-keep'      = '100',
    'write.metadata.delete-after-commit.enabled'= 'true',
    'write.metadata.previous-versions-max'      = '10',
    'impala.enable.stats.extrapolation'         = 'true'
);


/* --------------------------------------------------------------------------------------------
   5.6 — TABLA DE STAGING · Efímera, retención mínima
   --------------------------------------------------------------------------------------------
   Retención agresiva: no requiere time travel. Reduce costo de storage y de metadata.
   -------------------------------------------------------------------------------------------- */

CREATE TABLE IF NOT EXISTS telco_staging.stg_cdr_voz_reproceso (
    id_cdr                BIGINT,
    msisdn_origen         STRING,
    msisdn_destino        STRING,
    imsi                  STRING,
    id_celda_origen       STRING,
    id_celda_destino      STRING,
    ts_inicio             TIMESTAMP,
    ts_fin                TIMESTAMP,
    duracion_seg          INT,
    tipo_llamada          STRING,
    id_plan               INT,
    costo                 DECIMAL(18,6),
    id_operador_destino   STRING,
    codigo_terminacion    STRING,
    fecha_evento          DATE
)
PARTITIONED BY SPEC (DAYS(fecha_evento))
STORED AS ICEBERG
TBLPROPERTIES (
    'format-version'                            = '2',
    'write.delete.mode'                         = 'merge-on-read',
    'write.update.mode'                         = 'merge-on-read',
    'write.merge.mode'                          = 'merge-on-read',
    'write.format.default'                      = 'parquet',
    'write.parquet.compression-codec'           = 'snappy',      -- menor CPU, tabla efímera
    'history.expire.max-snapshot-age-ms'        = '86400000',    -- 1 día
    'history.expire.min-snapshots-to-keep'      = '3',
    'write.metadata.delete-after-commit.enabled'= 'true',
    'write.metadata.previous-versions-max'      = '3'
);


/* --------------------------------------------------------------------------------------------
   5.7 — TABLA DE ARCHIVO HISTÓRICO · Prefijo S3 desacoplado
   --------------------------------------------------------------------------------------------
   Destino del patrón copy-out (SECCIÓN 8.3). Este es el ÚNICO prefijo sobre el cual se
   pueden aplicar S3 Lifecycle Rules.
   -------------------------------------------------------------------------------------------- */

CREATE TABLE IF NOT EXISTS telco_archive.cdr_voz_historico (
    id_cdr                BIGINT,
    msisdn_origen         STRING,
    msisdn_destino        STRING,
    imsi                  STRING,
    id_celda_origen       STRING,
    id_celda_destino      STRING,
    ts_inicio             TIMESTAMP,
    ts_fin                TIMESTAMP,
    duracion_seg          INT,
    tipo_llamada          STRING,
    id_plan               INT,
    costo                 DECIMAL(18,6),
    id_operador_destino   STRING,
    codigo_terminacion    STRING,
    fecha_evento          DATE
)
PARTITIONED BY SPEC (MONTHS(fecha_evento))     -- granularidad mayor: acceso poco frecuente
STORED AS ICEBERG
TBLPROPERTIES (
    'format-version'                            = '2',
    'write.delete.mode'                         = 'merge-on-read',
    'write.update.mode'                         = 'merge-on-read',
    'write.merge.mode'                          = 'merge-on-read',
    'write.format.default'                      = 'parquet',
    'write.parquet.compression-codec'           = 'zstd',
    'write.target-file-size-bytes'              = '536870912',   -- 512 MB: menos archivos
    'history.expire.max-snapshot-age-ms'        = '604800000',
    'history.expire.min-snapshots-to-keep'      = '5',
    'write.metadata.delete-after-commit.enabled'= 'true',
    'write.metadata.previous-versions-max'      = '5'
);


