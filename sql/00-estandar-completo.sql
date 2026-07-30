/* ============================================================================================
   ESTÁNDAR OPERATIVO — APACHE ICEBERG v2 EN CLOUDERA CDW
   Diseño de tablas, retención de snapshots, borrado y mantenimiento
   Perfil: Clientes Telecomunicaciones (CDR, eventos de red, sesiones de datos)
   --------------------------------------------------------------------------------------------
   Motor objetivo : Impala (CDW Public Cloud) — ejecutable desde Hue / Cloudera Data Explorer
                    o impala-shell. Se indica explícitamente dónde se requiere Hive o Spark/CDE.
   Formato        : Apache Iceberg v2
   Versión doc    : 1.0
   Autor: Solutions Engineering - SSA
   ============================================================================================ */


/* ============================================================================================
   SECCIÓN 0 — HECHOS VERIFICADOS QUE CONDICIONAN TODO EL DISEÑO
   ============================================================================================

   ⚠ HECHO 0.1 — IMPALA SOLO ESCRIBE EN MERGE-ON-READ (MoR)
   --------------------------------------------------------------------------------------------
   Documentación Cloudera (Row-level operations):
     "Impala supports only the MOR mode and will fail if configured for copy-on-write.
      Impala does support reading copy-on-write tables."
      https://docs.cloudera.com/cdw-runtime/cloud/iceberg-how-to/topics/iceberg-row-level-ops.html 

   Implicaciones prácticas:
     · Un DELETE o UPDATE ejecutado DESDE IMPALA siempre genera position delete files.
     · Si se configura 'write.delete.mode'='copy-on-write' en una tabla, las operaciones
       de escritura desde Impala FALLARÁN. No es una degradación silenciosa: es un error.
     · Impala SÍ puede LEER tablas escritas en CoW por Hive o Spark.
     · Hive sí soporta ambos modos (CoW y MoR).
     · Spark/CDE sí soporta ambos modos.

   Conclusión de diseño:
     El modo de escritura NO se elige al azar — se elige en función de QUÉ MOTOR
     ejecuta los DELETE / UPDATE / MERGE sobre esa tabla. Ver SECCIÓN 2.


   ⚠ HECHO 0.2 — NINGÚN COMANDO DE BORRADO LIBERA ESPACIO EN S3/ADLS POR SÍ SOLO
   --------------------------------------------------------------------------------------------
   DELETE, UPDATE, MERGE, INSERT OVERWRITE y ALTER TABLE DROP PARTITION crean un nuevo
   snapshot. Los data files del snapshot anterior siguen físicamente en S3/ADLS hasta que ese
   snapshot expire. La liberación física SIEMPRE requiere EXPIRE_SNAPSHOTS posterior.


   ⚠ HECHO 0.3 — EXPIRE_SNAPSHOTS NO LIMPIA METADATA FILES POR DEFAULT
   --------------------------------------------------------------------------------------------
   Documentación Cloudera (Expire snapshots feature):
     "Expiring a snapshot does not remove old metadata files by default. You must clean up
      metadata files using write.metadata.delete-after-commit.enabled=true and
      write.metadata.previous-versions-max table properties."
      https://docs.cloudera.com/cdw-runtime/1.5.4/iceberg-how-to/topics/iceberg-expire-snapshots.html

   Sin estas dos propiedades, el prefijo /metadata/ crece indefinidamente aunque los data
   files sí se liberen.


   ⚠ HECHO 0.4 — EN TABLAS EXTERNAS, external.table.purge CONTROLA EL BORRADO FÍSICO
   --------------------------------------------------------------------------------------------
   Si la tabla es EXTERNAL_TABLE y external.table.purge NO está en 'true', Iceberg no
   elimina los archivos físicos de S3/ADLS. Los snapshots expiran del catálogo pero los parquets
   permanecen. Ver SECCIÓN 4.


   ⚠ HECHO 0.5 — EN MoR, EXPIRE_SNAPSHOTS SIN OPTIMIZE PREVIO NO LIBERA LOS DELETE FILES
   --------------------------------------------------------------------------------------------
   Un delete file referenciado por el snapshot activo NO es huérfano. REMOVE_ORPHAN_FILES
   no lo toca. Solo OPTIMIZE TABLE materializa los deletes y convierte los delete files en
   huérfanos elegibles para eliminación.

   Orden NO NEGOCIABLE en MoR:  OPTIMIZE → EXPIRE_SNAPSHOTS → REMOVE_ORPHAN_FILES


   ⚠ HECHO 0.6 — EL TIMESTAMP DE REMOVE_ORPHAN_FILES DEBE SER POSTERIOR AL OPTIMIZE
   --------------------------------------------------------------------------------------------
   REMOVE_ORPHAN_FILES('<ts>') solo elimina huérfanos creados ANTES de <ts>. Los delete
   files que el OPTIMIZE acaba de dejar huérfanos tienen timestamp de creación reciente.
   Si <ts> es anterior al momento del OPTIMIZE, esos archivos NO se eliminan y quedan
   acumulados en S3/ADLS indefinidamente.

   Regla práctica: usar un timestamp POSTERIOR al fin de la ventana de mantenimiento.


   ✘ OJO — NUNCA BORRAR ARCHIVOS DIRECTAMENTE DESDE S3/ADLS
   --------------------------------------------------------------------------------------------
   Eliminar objetos desde la consola de AWS/Azure, aws s3 rm, Azure Cli, o cualquier script rompe la
   integridad del catálogo. Snapshots activos quedan apuntando a archivos inexistentes.
   El daño puede ser irrecuperable. Sin excepciones, sin importar la urgencia.

   ============================================================================================ */


/* ============================================================================================
   SECCIÓN 1 — CREACIÓN DE BASE DE DATOS
   ============================================================================================ */

-- Base de datos operativa: tablas de alta frecuencia consultadas por BI y analítica
CREATE DATABASE IF NOT EXISTS telco_core
COMMENT 'Capa operativa — CDR, eventos de red y sesiones. Iceberg v2.'
LOCATION 's3a://<bucket-datalake>/warehouse/telco_core';

-- Base de datos de staging: landing zone de ingesta, tablas efímeras de reproceso
CREATE DATABASE IF NOT EXISTS telco_staging
COMMENT 'Staging — landing de ingesta y tablas temporales de reproceso.'
LOCATION 's3a://<bucket-datalake>/warehouse/telco_staging';

-- Base de datos de archivo: histórico frío, prefijo S3 DESACOPLADO para Lifecycle Rules
CREATE DATABASE IF NOT EXISTS telco_archive
COMMENT 'Archivo histórico frío. Prefijo desacoplado — apto para S3 Lifecycle Rules.'
LOCATION 's3a://<bucket-archive>/warehouse/telco_archive';

/* --------------------------------------------------------------------------------------------
   NOTA SOBRE SEPARACIÓN DE PREFIJOS S3
   --------------------------------------------------------------------------------------------
   telco_archive apunta a un BUCKET/PREFIJO DISTINTO de telco_core intencionalmente.
   Las S3 Lifecycle Rules (transición a Glacier, expiración) se aplican ÚNICAMENTE sobre
   el prefijo de archivo. Aplicarlas sobre el prefijo operativo destruiría data files que
   Iceberg aún referencia.
   -------------------------------------------------------------------------------------------- */


/* ============================================================================================
   SECCIÓN 2 — DECISIÓN: MERGE-ON-READ (MoR) vs COPY-ON-WRITE (CoW)
   ============================================================================================

   PRIMERA PREGUNTA (elimina la mayoría de los casos):
   ¿Qué motor ejecuta los DELETE / UPDATE / MERGE sobre esta tabla?

     · Solo Impala .......................... MoR obligatorio (CoW hace fallar la escritura)
     · Hive o Spark/CDE ..................... MoR o CoW, según patrón de uso
     · Mixto (Impala + Spark) .............. MoR obligatorio (el denominador común)

   SEGUNDA PREGUNTA (solo si el motor permite elegir):

   ┌──────────────────────────────┬──────────────────────┬──────────────────────────────────┐
   │ Característica del workload  │ Modo recomendado     │ Razón                            │
   ├──────────────────────────────┼──────────────────────┼──────────────────────────────────┤
   │ Ingesta streaming / CDC      │ MoR                  │ Escrituras eficientes            │
   │ Escritura frecuente (horaria)│ MoR                  │ Sin write amplification          │
   │ % de datos que cambia: bajo  │ MoR                  │ Pocos delete files acumulados    │
   │ Lecturas muy frecuentes      │ CoW                  │ Sin read amplification           │
   │ Updates/deletes masivos      │ CoW                  │ Evita miles de delete files      │
   │ Batch diario de actualización│ CoW                  │ Una reescritura vs N delete files│
   │ % de datos que cambia: alto  │ CoW                  │ Compactación deja de compensar   │
   └──────────────────────────────┴──────────────────────┴──────────────────────────────────┘

   APLICACIÓN AL PERFIL TELCO TÍPICO:

   ┌────────────────────────────────┬───────┬──────────────────────────────────────────────┐
   │ Tabla                          │ Modo  │ Justificación                                │
   ├────────────────────────────────┼───────┼──────────────────────────────────────────────┤
   │ cdr_voz / cdr_datos / cdr_sms  │ MoR   │ Ingesta nocturna masiva desde CDE. Lecturas  │
   │                                │       │ diurnas intensas → OPTIMIZE nocturno         │
   │                                │       │ obligatorio antes de la ventana de lectura.  │
   ├────────────────────────────────┼───────┼──────────────────────────────────────────────┤
   │ eventos_red_kpi                │ MoR   │ Append-only. Casi sin DELETE → los delete    │
   │                                │       │ files no se acumulan.                        │
   ├────────────────────────────────┼───────┼──────────────────────────────────────────────┤
   │ dim_suscriptor (SCD tipo 2)    │ CoW   │ Updates masivos diarios desde Spark/CDE.     │
   │                                │       │ Consultada en joins constantemente.          │
   │                                │       │ REQUIERE que los UPDATE corran en Spark/CDE  │
   │                                │       │ o Hive — NO desde Impala.                    │
   ├────────────────────────────────┼───────┼──────────────────────────────────────────────┤
   │ agregados_facturacion          │ CoW   │ Recálculo batch diario, alto % de cambio,    │
   │                                │       │ lectura constante por Finanzas.              │
   └────────────────────────────────┴───────┴──────────────────────────────────────────────┘

   ⚠ REGLA DE ORO: si existe cualquier posibilidad de que un operador ejecute un DELETE
     manual desde Hue/Impala sobre la tabla, la tabla DEBE ser MoR. Un CoW recibiendo un
     DELETE desde Impala falla la operación.

   ============================================================================================ */


/* ============================================================================================
   SECCIÓN 3 — CATÁLOGO DE PROPIEDADES DE TABLA
   ============================================================================================

   ── FORMATO ────────────────────────────────────────────────────────────────────────────────
   format-version = '2'
       Obligatorio para row-level deletes, MoR y time travel completo.

   write.format.default = 'parquet'
       Requerido por OPTIMIZE TABLE en Impala. Sin esto la compactación falla.

   write.parquet.compression-codec = 'zstd'
       Mejor ratio de compresión que snappy para CDR. Reduce egress y costo de storage.
       Alternativa conservadora: 'snappy' (menor CPU en escritura).

   write.target-file-size-bytes
       Tamaño objetivo de data file. Para CDR de alto volumen: 268435456 (256 MB).
       Para tablas de streaming con baja latencia: 134217728 (128 MB).

   ── MODO DE ESCRITURA ──────────────────────────────────────────────────────────────────────
   write.delete.mode  = 'merge-on-read' | 'copy-on-write'
   write.update.mode  = 'merge-on-read' | 'copy-on-write'
   write.merge.mode   = 'merge-on-read' | 'copy-on-write'
       ⚠ 'copy-on-write' hace FALLAR toda escritura desde Impala.
       Se pueden mezclar: p.ej. delete=MoR (borrados puntuales rápidos) +
       merge=CoW (upserts masivos sin generar miles de delete files).

   ── RETENCIÓN DE SNAPSHOTS ─────────────────────────────────────────────────────────────────
   history.expire.max-snapshot-age-ms
       Edad máxima de un snapshot antes de ser elegible para expiración.
         86400000    =  1 día
         604800000   =  7 días   ← recomendado para tablas operativas
         2592000000  = 30 días

   history.expire.min-snapshots-to-keep
       ⚠ ES UN CONTEO DE SNAPSHOTS, NO UN DELTA DE TIEMPO.
       Documentación Cloudera: si la tabla recibe 1 modificación por hora, min=24 cubre
       24 horas. Si recibe 1 por minuto, cubrir 24 horas requiere min=1440.

       Dimensionamiento para el perfil telco:
         · CDE inserta ~1 vez por minuto durante 6 h ≈ 360 snapshots/noche
         · Para conservar la noche completa: min-snapshots-to-keep = 400 (con margen)
         · Para conservar solo un punto de rollback: min-snapshots-to-keep = 5

   ── LIMPIEZA DE METADATA ───────────────────────────────────────────────────────────────────
   write.metadata.delete-after-commit.enabled = 'true'
       Sin esto, los metadata files NO se eliminan aunque los snapshots expiren.

   write.metadata.previous-versions-max = '10'
       Cuántas versiones de metadata conservar.

   ── TABLAS EXTERNAS ────────────────────────────────────────────────────────────────────────
   external.table.purge = 'true'
       Habilita que Iceberg elimine físicamente archivos de S3 en tablas EXTERNAL.
       Sin esta propiedad, EXPIRE_SNAPSHOTS y REMOVE_ORPHAN_FILES limpian el catálogo
       pero dejan los parquets en S3.

   ── ESTADÍSTICAS (IMPALA) ──────────────────────────────────────────────────────────────────
   impala.enable.stats.extrapolation = 'true'
       Requerido para usar COMPUTE STATS ... TABLESAMPLE. Sin esto:
         AnalysisException: COMPUTE STATS TABLESAMPLE requires stats extrapolation
         which is disabled.

   ============================================================================================ */


/* ============================================================================================
   SECCIÓN 4 — TABLAS INTERNAS (MANAGED) vs EXTERNAS: IMPLICACIONES EN MANTENIMIENTO
   ============================================================================================

   ┌───────────────────────┬─────────────────────────────┬──────────────────────────────────┐
   │ Aspecto               │ MANAGED (interna)           │ EXTERNAL (externa)               │
   ├───────────────────────┼─────────────────────────────┼──────────────────────────────────┤
   │ Ciclo de vida de      │ Gestionado por Iceberg       │ Depende de external.table.purge  │
   │ archivos en S3        │ completamente                │                                  │
   ├───────────────────────┼─────────────────────────────┼──────────────────────────────────┤
   │ EXPIRE_SNAPSHOTS      │ Elimina data files de S3     │ Elimina SOLO si purge='true'     │
   │ libera espacio        │                              │                                  │
   ├───────────────────────┼─────────────────────────────┼──────────────────────────────────┤
   │ REMOVE_ORPHAN_FILES   │ Elimina huérfanos de S3      │ Elimina SOLO si purge='true'     │
   ├───────────────────────┼─────────────────────────────┼──────────────────────────────────┤
   │ DROP TABLE            │ Borra datos                  │ Borra datos SOLO si purge='true' │
   ├───────────────────────┼─────────────────────────────┼──────────────────────────────────┤
   │ Cuándo usarla         │ Tablas operativas cuyo ciclo │ Datos compartidos con otros      │
   │                       │ de vida gestiona la          │ consumidores fuera de CDW, o     │
   │                       │ plataforma                   │ tablas preexistentes en S3       │
   └───────────────────────┴─────────────────────────────┴──────────────────────────────────┘

   ⚠ CAUSA RAÍZ FRECUENTE DE "LOS ARCHIVOS NO SE BORRAN DE S3":
     La tabla es EXTERNAL y external.table.purge no está en 'true'. El mantenimiento se
     ejecuta sin error, los snapshots desaparecen del catálogo, pero los parquets siguen
     en S3 acumulando costo.

   VERIFICACIÓN OBLIGATORIA ANTES DE DIAGNOSTICAR CUALQUIER PROBLEMA DE STORAGE:
   -------------------------------------------------------------------------------------------- */

DESCRIBE FORMATTED telco_core.<tabla>;
-- Revisar en la salida:
--   Table Type:            MANAGED_TABLE | EXTERNAL_TABLE
--   external.table.purge   true | false | (ausente)
--   write.delete.mode      merge-on-read | copy-on-write
--   format-version         2


/* ============================================================================================
   SECCIÓN 5 — DDL: CREACIÓN DE TABLAS
   ============================================================================================

   SINTAXIS POR MOTOR (diferencia sutil pero real):
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


/* ============================================================================================
   SECCIÓN 6 — SALVEDADES DE PARTICIONAMIENTO
   ============================================================================================

   6.1 — LA GRANULARIDAD DEBE COINCIDIR CON LA UNIDAD MÍNIMA DE PURGA/REPROCESO
   --------------------------------------------------------------------------------------------
   Si los reprocesos son por día, particionar por DAYS(). Si se particiona por MONTHS() y
   se reprocesa un día, DROP PARTITION deja de ser viable y el único camino es DELETE →
   delete files → penalización de lectura en MoR.

   ┌───────────────────────────────┬────────────────────────────────────────────────────────┐
   │ Frecuencia de purga/reproceso │ Partition spec                                         │
   ├───────────────────────────────┼────────────────────────────────────────────────────────┤
   │ Diaria (típico CDR)           │ PARTITIONED BY SPEC (DAYS(fecha_evento))               │
   │ Mensual (ciclos facturación)  │ PARTITIONED BY SPEC (MONTHS(fecha_corte))              │
   │ Anual (histórico frío)        │ PARTITIONED BY SPEC (YEARS(fecha_evento))              │
   │ Diaria + alto volumen         │ PARTITIONED BY SPEC (DAYS(fecha), BUCKET(16, msisdn))  │
   └───────────────────────────────┴────────────────────────────────────────────────────────┘


   6.2 — REGLAS DEL FILTRO EN DROP PARTITION (documentación Cloudera)
   --------------------------------------------------------------------------------------------
   · El filtro debe ser combinación de, o al menos uno de:
       - un predicado binario
       - un predicado IN
       - un predicado IS NULL
   · El argumento del predicado DEBE ser un partition transform: DAYS(col), MONTHS(col),
     YEARS(col), BUCKET(n, col), TRUNCATE(n, col) — o una columna identity.
   · Todo transform no-identity DEBE aparecer en el filtro tal como está en el partition spec.
     Si la tabla se particiona por DAYS(fecha), el filtro debe usar DAYS(fecha) — no fecha.
   · Operadores soportados: =, !=, <, >, <=, >=


   6.3 — NO SOBRE-PARTICIONAR
   --------------------------------------------------------------------------------------------
   Particionar por hora en una tabla de 5 años genera ~43.800 particiones. El overhead de
   metadata y planificación supera el beneficio de pruning. Para CDR de alto volumen,
   DAYS() + BUCKET() distribuye mejor que particiones horarias.


   6.4 — EVOLUCIÓN DE PARTITION SPEC
   --------------------------------------------------------------------------------------------
   Iceberg permite cambiar el partition spec sin reescribir datos. Los data files antiguos
   conservan el spec anterior. Para unificar el layout, ejecutar OPTIMIZE TABLE SIN el
   parámetro FILE_SIZE_THRESHOLD_MB — solo la compactación completa reescribe todo según
   el spec vigente.

   ⚠ Con FILE_SIZE_THRESHOLD_MB especificado, los archivos que no cumplen el umbral
     conservan el esquema y layout de partición antiguos.

   ============================================================================================ */

-- Inspección del partition spec vigente
SHOW PARTITIONS telco_core.cdr_voz;
SHOW CREATE TABLE telco_core.cdr_voz;


/* ============================================================================================
   SECCIÓN 7 — BORRADO DE DATOS
   ============================================================================================ */

/* --------------------------------------------------------------------------------------------
   7.1 — MÉTODO PREFERIDO: ALTER TABLE DROP PARTITION
   --------------------------------------------------------------------------------------------
   Ventaja decisiva en MoR: NO genera delete files. Elimina la partición a nivel de snapshot.
   Las lecturas concurrentes no pagan overhead de merge-on-read.
   -------------------------------------------------------------------------------------------- */

-- Una partición específica
ALTER TABLE telco_core.cdr_voz
    DROP PARTITION (DAYS(fecha_evento) = '2026-06-15');

-- Rango de fechas (los operadores <, >, <=, >= son soportados)
ALTER TABLE telco_core.cdr_voz
    DROP PARTITION (DAYS(fecha_evento) < '2026-01-01');

-- Rango acotado — dos predicados binarios combinados
ALTER TABLE telco_core.cdr_voz
    DROP PARTITION (DAYS(fecha_evento) >= '2026-06-01'
                AND DAYS(fecha_evento) <= '2026-06-30');

-- Partición mensual
ALTER TABLE telco_core.agregados_facturacion
    DROP PARTITION (MONTHS(fecha_corte) = '2026-03');

/* ⚠ DROP PARTITION NO LIBERA ESPACIO EN S3 POR SÍ SOLO.
     Crea un snapshot sin esa partición. Los data files siguen referenciados por el
     snapshot anterior hasta que se ejecute EXPIRE_SNAPSHOTS. Ver SECCIÓN 9. */


/* --------------------------------------------------------------------------------------------
   7.2 — MÉTODO ALTERNATIVO: DELETE (solo cuando DROP PARTITION no aplica)
   --------------------------------------------------------------------------------------------
   Usar únicamente cuando el criterio de borrado NO coincide con límites de partición.
   -------------------------------------------------------------------------------------------- */

-- Borrado que cruza particiones parcialmente (DROP PARTITION no aplica)
DELETE FROM telco_core.cdr_voz
WHERE codigo_terminacion = 'ERR_TIMEOUT'
  AND fecha_evento BETWEEN DATE '2026-06-01' AND DATE '2026-06-30';

-- Borrado por criterio de negocio no alineado a partición (GDPR / derecho al olvido)
DELETE FROM telco_core.cdr_voz
WHERE msisdn_origen = '<msisdn_solicitante>';

/* ⚠ CONSECUENCIAS DEL DELETE EN MoR:
     · Genera position delete files inmediatamente.
     · TODAS las lecturas concurrentes deben aplicar esos delete files en tiempo de lectura.
     · Los delete files NO desaparecen con EXPIRE_SNAPSHOTS ni con REMOVE_ORPHAN_FILES:
       están referenciados por el snapshot activo, por lo tanto NO son huérfanos.
     · Solo OPTIMIZE TABLE los materializa y los convierte en huérfanos elegibles.

   Si un DELETE corre durante la ventana de lectura diurna, el overhead persiste hasta el
   siguiente OPTIMIZE — potencialmente 18-20 horas. */


/* --------------------------------------------------------------------------------------------
   7.3 — ANTIPATRONES DE BORRADO
   --------------------------------------------------------------------------------------------

   ✘ Borrar objetos directamente en S3 (consola AWS, aws s3 rm, scripts)
       → Rompe el catálogo. Snapshots apuntan a archivos inexistentes. Daño potencialmente
         irrecuperable.

   ✘ DELETE cuando el criterio coincide exactamente con una partición
       → Genera delete files innecesarios. Usar DROP PARTITION.

   ✘ INSERT OVERWRITE asumiendo semántica Hive
       → En Iceberg NO reemplaza archivos físicos. Crea un snapshot nuevo y deja los
         anteriores como huérfanos pendientes de expiración.

   ✘ DROP PARTITION o DELETE sin EXPIRE_SNAPSHOTS posterior
       → El espacio en S3 nunca se libera. Crecimiento silencioso de costo.

   ✘ EXPIRE_SNAPSHOTS en tabla MoR sin OPTIMIZE previo
       → Los delete files sobreviven. El espacio no se libera.

   ✘ DELETE / UPDATE desde Impala sobre tabla configurada en copy-on-write
       → La operación FALLA. Ver SECCIÓN 0, hecho 0.1.

   -------------------------------------------------------------------------------------------- */


/* ============================================================================================
   SECCIÓN 8 — REPROCESO DE PARTICIONES
   ============================================================================================ */

/* --------------------------------------------------------------------------------------------
   8.1 — PATRÓN RECOMENDADO: DROP PARTITION + INSERT
   --------------------------------------------------------------------------------------------
   No genera delete files. Es el patrón correcto para reprocesos durante ventana de lectura.
   -------------------------------------------------------------------------------------------- */

-- Paso 1 · Validar el volumen del origen ANTES de destruir el destino
SELECT fecha_evento, COUNT(*) AS registros
FROM telco_staging.stg_cdr_voz_reproceso
WHERE fecha_evento BETWEEN DATE '2026-06-01' AND DATE '2026-06-03'
GROUP BY fecha_evento
ORDER BY fecha_evento;

-- Paso 2 · Registrar el conteo actual del destino (para conciliación posterior)
SELECT fecha_evento, COUNT(*) AS registros_antes
FROM telco_core.cdr_voz
WHERE fecha_evento BETWEEN DATE '2026-06-01' AND DATE '2026-06-03'
GROUP BY fecha_evento
ORDER BY fecha_evento;

-- Paso 3 · Eliminar las particiones a reprocesar
ALTER TABLE telco_core.cdr_voz
    DROP PARTITION (DAYS(fecha_evento) >= '2026-06-01'
                AND DAYS(fecha_evento) <= '2026-06-03');

-- Paso 4 · Recargar (Iceberg usa hidden partitioning: no se declara la partición en el INSERT)
INSERT INTO telco_core.cdr_voz
SELECT
    id_cdr, msisdn_origen, msisdn_destino, imsi,
    id_celda_origen, id_celda_destino,
    ts_inicio, ts_fin, duracion_seg, tipo_llamada,
    id_plan, costo, id_operador_destino, codigo_terminacion,
    fecha_evento
FROM telco_staging.stg_cdr_voz_reproceso
WHERE fecha_evento BETWEEN DATE '2026-06-01' AND DATE '2026-06-03';

-- Paso 5 · Conciliar
SELECT fecha_evento, COUNT(*) AS registros_despues
FROM telco_core.cdr_voz
WHERE fecha_evento BETWEEN DATE '2026-06-01' AND DATE '2026-06-03'
GROUP BY fecha_evento
ORDER BY fecha_evento;

-- Paso 6 · Liberar S3 (opcional inmediato si las particiones son grandes;
--          si no, el mantenimiento nocturno lo cubre)
ALTER TABLE telco_core.cdr_voz
    EXECUTE EXPIRE_SNAPSHOTS(NOW() - INTERVAL 1 HOURS);

/* NOTA: OPTIMIZE no es necesario en este flujo. DROP PARTITION + INSERT no genera
   delete files — no hay nada que materializar. */


/* --------------------------------------------------------------------------------------------
   8.2 — REPROCESO CUANDO DROP PARTITION NO APLICA
   --------------------------------------------------------------------------------------------
   Si el criterio de reproceso no coincide con límites de partición, el DELETE es inevitable.
   En ese caso, ejecutar OPTIMIZE inmediatamente después para no arrastrar delete files
   durante toda la ventana de lectura.
   -------------------------------------------------------------------------------------------- */

DELETE FROM telco_core.cdr_voz
WHERE fecha_evento = DATE '2026-06-15'
  AND id_operador_destino = 'OPERADOR_X';

INSERT INTO telco_core.cdr_voz
SELECT * FROM telco_staging.stg_cdr_voz_reproceso
WHERE fecha_evento = DATE '2026-06-15'
  AND id_operador_destino = 'OPERADOR_X';

-- ⚠ OBLIGATORIO tras un DELETE durante ventana de lectura:
--   compactar para eliminar el overhead de merge-on-read en las consultas del resto del día.
--   El umbral evita reescribir la tabla completa.
OPTIMIZE TABLE telco_core.cdr_voz FILE_SIZE_THRESHOLD_MB=256;


/* --------------------------------------------------------------------------------------------
   8.3 — ARCHIVADO FRÍO: copy-out → prune → expire
   --------------------------------------------------------------------------------------------
   Único patrón seguro para mover histórico a almacenamiento de bajo costo sin tocar S3
   manualmente.
   -------------------------------------------------------------------------------------------- */

-- Paso 1 · Copiar a la tabla de archivo (prefijo S3 desacoplado)
INSERT INTO telco_archive.cdr_voz_historico
SELECT * FROM telco_core.cdr_voz
WHERE fecha_evento < DATE '2024-01-01';

-- Paso 2 · Conciliar antes de destruir el origen
SELECT
    (SELECT COUNT(*) FROM telco_core.cdr_voz
      WHERE fecha_evento < DATE '2024-01-01')          AS origen,
    (SELECT COUNT(*) FROM telco_archive.cdr_voz_historico
      WHERE fecha_evento < DATE '2024-01-01')          AS destino;

-- Paso 3 · Eliminar del operativo (solo si el conteo cuadra)
ALTER TABLE telco_core.cdr_voz
    DROP PARTITION (DAYS(fecha_evento) < '2024-01-01');

-- Paso 4 · Expirar snapshots del operativo — SIN ESTE PASO S3 NO LIBERA NADA
ALTER TABLE telco_core.cdr_voz
    EXECUTE EXPIRE_SNAPSHOTS(NOW() - INTERVAL 1 HOURS);

-- Paso 5 · Limpiar huérfanos
ALTER TABLE telco_core.cdr_voz
    EXECUTE REMOVE_ORPHAN_FILES(NOW() - INTERVAL 1 HOURS);

/* ⚠ S3 LIFECYCLE RULES
   Aplicar ÚNICAMENTE sobre s3://<bucket-archive>/warehouse/telco_archive/
   NUNCA sobre el prefijo operativo. Los archivos del operativo son gestionados
   exclusivamente por el garbage collector de Iceberg. */


/* ============================================================================================
   SECCIÓN 9 — PROCESOS DE MANTENIMIENTO
   ============================================================================================

   SECUENCIA CANÓNICA — EL ORDEN NO ES NEGOCIABLE

     ① OPTIMIZE TABLE            → materializa delete files, compacta archivos pequeños
     ② EXPIRE_SNAPSHOTS          → elimina snapshots viejos y sus data files exclusivos
     ③ REMOVE_ORPHAN_FILES       → elimina archivos sin referencia en ningún snapshot
     ④ COMPUTE STATS             → refresca estadísticas del optimizador de Impala
     ⑤ INVALIDATE METADATA       → SOLO si el mantenimiento corrió desde CDE/Spark

   ============================================================================================ */


/* --------------------------------------------------------------------------------------------
   9.1 — OPTIMIZE TABLE (compactación)
   --------------------------------------------------------------------------------------------
   Sintaxis oficial:  OPTIMIZE TABLE [db.]tabla [FILE_SIZE_THRESHOLD_MB=<valor>]

   Tareas que ejecuta:
     · Fusiona delete files con sus data files correspondientes (crítico en MoR)
     · Compacta data files menores al umbral especificado
     · Sin umbral: reescribe TODA la tabla y la convierte al esquema y partition spec vigentes

   Requisitos:
     · write.format.default debe ser 'parquet'
     · El usuario necesita privilegio ALL sobre la tabla
     · No aplica sobre vistas

   ⚠ Impala NO soporta filtro de partición en OPTIMIZE TABLE. Reescribe toda la tabla.
     Para compactación filtrada por partición se requiere Spark/CDE (ver 9.1.3).
   -------------------------------------------------------------------------------------------- */

-- 9.1.1 · Mantenimiento recurrente — CON umbral (recomendado, ahorra cómputo)
-- Selecciona solo archivos pequeños y delete files; conserva los archivos ya bien dimensionados.
OPTIMIZE TABLE telco_core.cdr_voz FILE_SIZE_THRESHOLD_MB=256;

-- 9.1.2 · Compactación completa — SIN umbral
-- Necesaria solo tras evolución de esquema o de partition spec. Costosa en tablas grandes.
OPTIMIZE TABLE telco_core.cdr_voz;

/* 9.1.3 · Compactación filtrada por partición — requiere Spark/CDE
   -------------------------------------------------------------------------------------------
   CALL spark_catalog.system.rewrite_data_files(
       table    => 'telco_core.cdr_voz',
       strategy => 'binpack',
       where    => 'fecha_evento = "2026-06-15"'
   );

   ⚠ Verificado con reserva: el parámetro `where` existe en la especificación Iceberg, pero
     hay reportes de fallo (NoSuchElementException) cuando el filtro involucra partition
     transforms como DAYS()/MONTHS(). Validar en el ambiente antes de incorporarlo a un job
     productivo. Si falla, usar OPTIMIZE TABLE completo desde Impala. */


/* --------------------------------------------------------------------------------------------
   9.2 — EXPIRE_SNAPSHOTS
   --------------------------------------------------------------------------------------------
   Sintaxis oficial Hive/Impala (cuatro variantes soportadas):
     ALTER TABLE <tbl> EXECUTE EXPIRE_SNAPSHOTS(<expresión timestamp>)
     ALTER TABLE <tbl> EXECUTE EXPIRE_SNAPSHOTS('<snapshot_id>')
     ALTER TABLE <tbl> EXECUTE EXPIRE_SNAPSHOTS('<snapshot_id1>,<snapshot_id2>,...')
     ALTER TABLE <tbl> EXECUTE EXPIRE_SNAPSHOTS BETWEEN (<ts1>) AND (<ts2>)
   -------------------------------------------------------------------------------------------- */

-- 9.2.1 · Por timestamp absoluto
ALTER TABLE telco_core.cdr_voz
    EXECUTE EXPIRE_SNAPSHOTS('2026-07-20 06:00:00');

-- 9.2.2 · Por expresión relativa (preferido para jobs automatizados — no requiere parametrizar)
ALTER TABLE telco_core.cdr_voz
    EXECUTE EXPIRE_SNAPSHOTS(NOW() - INTERVAL 7 DAYS);

-- 9.2.3 · Snapshot específico (p. ej. eliminar un snapshot que contiene datos bajo GDPR)
ALTER TABLE telco_core.cdr_voz
    EXECUTE EXPIRE_SNAPSHOTS('3821459023847562930');

-- 9.2.4 · Rango entre dos timestamps
ALTER TABLE telco_core.cdr_voz
    EXECUTE EXPIRE_SNAPSHOTS BETWEEN ('2026-06-01 00:00:00') AND ('2026-06-30 23:59:59');

/* ⚠ PROTECCIÓN CONTRA EXPIRACIÓN EXCESIVA
   history.expire.min-snapshots-to-keep actúa como red de seguridad: aunque el timestamp
   cubra todos los snapshots, se conserva ese número mínimo.
   Recordar que es un CONTEO, no un delta de tiempo — dimensionarlo según la frecuencia
   real de commits de la tabla (ver SECCIÓN 3). */


/* --------------------------------------------------------------------------------------------
   9.3 — REMOVE_ORPHAN_FILES
   --------------------------------------------------------------------------------------------
   Elimina archivos presentes en el directorio de la tabla que no están referenciados por
   ningún snapshot. Comando de Impala.
   -------------------------------------------------------------------------------------------- */

ALTER TABLE telco_core.cdr_voz
    EXECUTE REMOVE_ORPHAN_FILES(NOW() - INTERVAL 1 HOURS);

/* ⚠ EL TIMESTAMP ES LA CAUSA MÁS FRECUENTE DE FALLO SILENCIOSO
   REMOVE_ORPHAN_FILES('<ts>') solo elimina huérfanos CREADOS ANTES de <ts>. Los delete
   files que el OPTIMIZE acaba de dejar huérfanos tienen timestamp de creación reciente.

   Ejemplo de error real:
     06:00  OPTIMIZE TABLE ...                              → deja delete files huérfanos
     06:30  EXECUTE EXPIRE_SNAPSHOTS('2026-07-27 00:00:00')
     06:31  EXECUTE REMOVE_ORPHAN_FILES('2026-07-27 00:00:00')
            → NO elimina nada: los huérfanos se crearon a las 06:00, después de las 00:00.
            → Los delete files quedan acumulados en S3 indefinidamente.

   Regla: el timestamp de REMOVE_ORPHAN_FILES debe ser POSTERIOR al fin del OPTIMIZE.
   Usar expresiones relativas (NOW() - INTERVAL 1 HOURS) elimina esta clase de error. */


/* --------------------------------------------------------------------------------------------
   9.4 — ESTADÍSTICAS PARA EL OPTIMIZADOR DE IMPALA
   --------------------------------------------------------------------------------------------
   Son DOS capas independientes:
     · Estadísticas de Iceberg (manifest files) → pruning de archivos. Automáticas.
     · Estadísticas de Impala (HMS)            → plan de ejecución, orden de joins,
                                                  estimación de cardinalidad. Manuales.

   El warning "missing relevant table and/or column statistics" en el plan de ejecución
   se refiere a la SEGUNDA capa. Iceberg tener estadísticas propias no lo elimina.

   ⚠ COMPUTE INCREMENTAL STATS NO ES FUNCIONAL EN ICEBERG.
     Impala lo convierte internamente en un COMPUTE STATS completo sobre toda la tabla.
   -------------------------------------------------------------------------------------------- */

-- 9.4.1 · Tablas pequeñas y medianas: stats completas
COMPUTE STATS telco_core.dim_suscriptor;

-- 9.4.2 · Tablas de miles de millones de registros: sampling
-- Prerrequisito: habilitar extrapolación (una sola vez por tabla; es cambio de metadata,
-- instantáneo, independiente del tamaño de la tabla).
ALTER TABLE telco_core.cdr_voz
    SET TBLPROPERTIES ('impala.enable.stats.extrapolation'='true');

-- Sampling al 10% — porcentajes muy por debajo de 10% dan resultados pobres.
-- Restringir a las columnas que participan en filtros, joins y GROUP BY.
COMPUTE STATS telco_core.cdr_voz
    (fecha_evento, msisdn_origen, id_celda_origen, tipo_llamada, id_plan)
TABLESAMPLE SYSTEM(10);

/* Sobre REPEATABLE(<semilla>):
     COMPUTE STATS ... TABLESAMPLE SYSTEM(10) REPEATABLE(42);
   Fija la selección de archivos para que dos ejecuciones sean comparables. Útil para
   validar consistencia entre corridas.
   ⚠ En tablas con ingesta incremental continua, una semilla fija puede seleccionar
     siempre los mismos archivos históricos y excluir los nuevos. Para tablas CDR con
     carga diaria, OMITIR la semilla.

   Frecuencia de recálculo: cuando ~30% de las filas hayan cambiado. Si se recarga un
   volumen similar sin variar significativamente el número de filas ni la cardinalidad
   por columna, no es necesario recalcular. */

-- Verificación
SHOW TABLE STATS telco_core.cdr_voz;
SHOW COLUMN STATS telco_core.cdr_voz;


/* --------------------------------------------------------------------------------------------
   9.5 — SINCRONIZACIÓN DE CATÁLOGO (INVALIDATE METADATA)
   --------------------------------------------------------------------------------------------
   · Mantenimiento ejecutado desde Impala/Hue → NO se requiere. El Catalog Service de CDW
     propaga los cambios a todos los coordinadores automáticamente.
   · Mantenimiento ejecutado desde CDE/Spark → ejecutar INVALIDATE METADATA desde Impala.
     El HMS event polling sincroniza eventualmente, pero no garantiza inmediatez: para un
     job nocturno seguido de lecturas al amanecer, el comando explícito es más seguro.
   -------------------------------------------------------------------------------------------- */

INVALIDATE METADATA telco_core.cdr_voz;

/* Automatización desde un job CDE (CDP Public Cloud usa LDAP + workload password,
   NO Kerberos). Paso final en Python con impyla:

   from impala.dbapi import connect
   import os

   conn = connect(
       host               = os.environ["IMPALA_VW_HOST"],      # de CDW → Copy JDBC String
       port               = 443,
       auth_mechanism     = "LDAP",
       use_ssl            = True,
       use_http_transport = True,
       http_path          = os.environ["IMPALA_HTTP_PATH"],
       user               = os.environ["CDP_WORKLOAD_USER"],
       password           = os.environ["CDP_WORKLOAD_PASSWORD"]
   )
   cur = conn.cursor()
   for t in ["telco_core.cdr_voz", "telco_core.cdr_datos"]:
       cur.execute(f"INVALIDATE METADATA {t}")
   cur.close(); conn.close()

   Las credenciales se inyectan como variables de entorno del job — nunca en el código. */


/* --------------------------------------------------------------------------------------------
   9.6 — SECUENCIA COMPLETA DE MANTENIMIENTO (PLANTILLA EJECUTABLE)
   -------------------------------------------------------------------------------------------- */

-- ① Compactar (materializa delete files — CRÍTICO en MoR)
OPTIMIZE TABLE telco_core.cdr_voz FILE_SIZE_THRESHOLD_MB=256;

-- ② Expirar snapshots (libera data files de snapshots antiguos)
ALTER TABLE telco_core.cdr_voz
    EXECUTE EXPIRE_SNAPSHOTS(NOW() - INTERVAL 7 DAYS);

-- ③ Eliminar huérfanos (timestamp POSTERIOR al OPTIMIZE — usar expresión relativa corta)
ALTER TABLE telco_core.cdr_voz
    EXECUTE REMOVE_ORPHAN_FILES(NOW() - INTERVAL 1 HOURS);

-- ④ Refrescar estadísticas (solo si hubo >30% de cambio en los datos)
COMPUTE STATS telco_core.cdr_voz
    (fecha_evento, msisdn_origen, id_celda_origen, tipo_llamada, id_plan)
TABLESAMPLE SYSTEM(10);

-- ⑤ Sincronizar catálogo (solo si los pasos anteriores corrieron desde CDE/Spark)
-- INVALIDATE METADATA telco_core.cdr_voz;


/* ============================================================================================
   SECCIÓN 10 — CUÁNDO EJECUTAR EL MANTENIMIENTO
   ============================================================================================

   10.1 — PRINCIPIO RECTOR
   --------------------------------------------------------------------------------------------
   El mantenimiento se agenda en la ventana de MENOR CONCURRENCIA, e inmediatamente ANTES
   de la ventana de mayor lectura. El objetivo es que las consultas del día encuentren la
   tabla ya compactada, sin delete files pendientes.


   10.2 — CALENDARIO PARA EL PERFIL TELCO TÍPICO
   --------------------------------------------------------------------------------------------

     00:00 ─ 06:00   CDE escribe CDRs (ingesta masiva, MoR)
     06:00 ─ 07:00   ██ VENTANA DE MANTENIMIENTO ██
                     ① OPTIMIZE con FILE_SIZE_THRESHOLD_MB
                     ② EXPIRE_SNAPSHOTS
                     ③ REMOVE_ORPHAN_FILES
                     ④ COMPUTE STATS (condicional)
                     ⑤ INVALIDATE METADATA (si corrió desde CDE)
     07:00 ─ 23:59   CDW atiende lecturas — tabla compactada, sin overhead de merge-on-read
                     └── Reprocesos diurnos: usar DROP PARTITION + INSERT (SECCIÓN 8.1)
                         Si se usa DELETE, ejecutar OPTIMIZE inmediatamente después.


   10.3 — FRECUENCIA POR TIPO DE TABLA
   --------------------------------------------------------------------------------------------
   ┌──────────────────────────────┬──────────────┬────────────────────────────────────────┐
   │ Perfil de la tabla           │ OPTIMIZE     │ EXPIRE_SNAPSHOTS                       │
   ├──────────────────────────────┼──────────────┼────────────────────────────────────────┤
   │ CDR alta frecuencia (MoR)    │ Diario       │ Diario · NOW() - INTERVAL 7 DAYS       │
   │ Streaming / CDC (MoR)        │ Cada 4-6 h   │ Diario · NOW() - INTERVAL 1 DAYS       │
   │ Dimensión batch diario (CoW) │ Semanal      │ Diario · NOW() - INTERVAL 30 DAYS      │
   │ Agregados mensuales          │ Mensual      │ Mensual · NOW() - INTERVAL 30 DAYS     │
   │ Staging efímero              │ No requerido │ Diario · NOW() - INTERVAL 1 DAYS       │
   │ Archivo histórico            │ Tras carga   │ Tras carga · NOW() - INTERVAL 1 DAYS   │
   └──────────────────────────────┴──────────────┴────────────────────────────────────────┘

   REMOVE_ORPHAN_FILES: misma frecuencia que EXPIRE_SNAPSHOTS, siempre inmediatamente
   después, con timestamp posterior al OPTIMIZE de esa misma ejecución.


   10.4 — DISPARADORES ADICIONALES (fuera de calendario)
   --------------------------------------------------------------------------------------------
   · Tras un DELETE masivo durante ventana de lectura → OPTIMIZE inmediato
   · Tras evolución de esquema o partition spec       → OPTIMIZE completo (sin umbral)
   · Tras un reproceso de particiones grandes         → EXPIRE_SNAPSHOTS + REMOVE_ORPHAN_FILES
   · Cuando el diagnóstico 11.2 muestre acumulación de delete files → OPTIMIZE

   ============================================================================================ */


/* ============================================================================================
   SECCIÓN 11 — QUERIES DE DIAGNÓSTICO Y VERIFICACIÓN
   ============================================================================================ */

-- 11.1 · Inventario de snapshots activos
SELECT snapshot_id, committed_at, operation
FROM telco_core.cdr_voz.snapshots
ORDER BY committed_at DESC;

-- 11.2 · Acumulación de archivos por tipo
--        content = 0 → data file
--        content = 1 → position delete file
--        content = 2 → equality delete file
-- Si content 1 o 2 aparecen con volumen significativo, la tabla necesita OPTIMIZE.
SELECT
    content,
    COUNT(*)                            AS archivos,
    SUM(record_count)                   AS registros,
    ROUND(SUM(file_size_in_bytes)/1024/1024, 2) AS mb_total
FROM telco_core.cdr_voz.files
GROUP BY content
ORDER BY content;

-- 11.3 · Distribución de tamaño de data files (detecta el problema de small files)
SELECT
    CASE
        WHEN file_size_in_bytes < 8388608   THEN '01 · < 8 MB'
        WHEN file_size_in_bytes < 33554432  THEN '02 · 8-32 MB'
        WHEN file_size_in_bytes < 134217728 THEN '03 · 32-128 MB'
        WHEN file_size_in_bytes < 268435456 THEN '04 · 128-256 MB'
        ELSE                                     '05 · > 256 MB'
    END                                  AS rango,
    COUNT(*)                             AS archivos,
    ROUND(SUM(file_size_in_bytes)/1024/1024/1024, 2) AS gb_total
FROM telco_core.cdr_voz.files
WHERE content = 0
GROUP BY 1
ORDER BY 1;

-- 11.4 · Historial de operaciones (identifica qué tipo de escritura domina la tabla)
SELECT operation, COUNT(*) AS veces,
       MIN(committed_at) AS primera, MAX(committed_at) AS ultima
FROM telco_core.cdr_voz.snapshots
GROUP BY operation
ORDER BY veces DESC;

-- 11.5 · Verificación de configuración de la tabla
--        Revisar: Table Type, external.table.purge, write.delete.mode, format-version,
--                 history.expire.*, write.metadata.*
DESCRIBE FORMATTED telco_core.cdr_voz;

-- 11.6 · Partition spec vigente
SHOW PARTITIONS telco_core.cdr_voz;

-- 11.7 · Historial completo de la tabla
SELECT * FROM telco_core.cdr_voz.history ORDER BY made_current_at DESC;

-- 11.8 · Manifiestos activos
SELECT * FROM telco_core.cdr_voz.manifests;


/* ============================================================================================
   SECCIÓN 12 — RUNBOOK: "EL MANTENIMIENTO CORRIÓ PERO S3 NO LIBERÓ ESPACIO"
   ============================================================================================

   Ejecutar en este orden. Detenerse en el primer hallazgo positivo.

   ── PASO 1 · ¿La tabla es EXTERNAL sin purge habilitado?
   --------------------------------------------------------------------------------------------
     DESCRIBE FORMATTED <tabla>;
     Buscar:  Table Type = EXTERNAL_TABLE  Y  external.table.purge ausente o 'false'
     → CAUSA CONFIRMADA. Corregir:
         ALTER TABLE <tabla> SET TBLPROPERTIES ('external.table.purge'='true');
       Luego re-ejecutar la secuencia completa de mantenimiento.

   ── PASO 2 · ¿Quedan delete files activos? (tabla MoR)
   --------------------------------------------------------------------------------------------
     SELECT content, COUNT(*) FROM <tabla>.files WHERE content IN (1,2) GROUP BY content;
     → Si retorna filas: los delete files están REFERENCIADOS por el snapshot activo.
       No son huérfanos. REMOVE_ORPHAN_FILES nunca los tocará.
       Corregir: ejecutar OPTIMIZE TABLE, luego EXPIRE_SNAPSHOTS, luego REMOVE_ORPHAN_FILES.

   ── PASO 3 · ¿El timestamp de REMOVE_ORPHAN_FILES fue anterior al OPTIMIZE?
   --------------------------------------------------------------------------------------------
     Comparar la hora de ejecución del OPTIMIZE contra el timestamp pasado al comando.
     → Si el timestamp es anterior: los huérfanos recién creados quedaron fuera de rango.
       Corregir: re-ejecutar con un timestamp posterior.
         ALTER TABLE <tabla> EXECUTE REMOVE_ORPHAN_FILES(NOW() - INTERVAL 1 HOURS);

   ── PASO 4 · ¿Los snapshots realmente expiraron?
   --------------------------------------------------------------------------------------------
     SELECT snapshot_id, committed_at FROM <tabla>.snapshots ORDER BY committed_at ASC;
     → Si aún aparecen snapshots del período que se pretendía expirar:
       revisar history.expire.min-snapshots-to-keep. Recordar que es un CONTEO: un valor
       alto en una tabla con commits frecuentes retiene mucho más tiempo del esperado.

   ── PASO 5 · ¿Los archivos son metadata, no data?
   --------------------------------------------------------------------------------------------
     Revisar si los objetos remanentes están bajo el prefijo /metadata/ (.avro, .json).
     → EXPIRE_SNAPSHOTS no limpia metadata por default. Corregir:
         ALTER TABLE <tabla> SET TBLPROPERTIES (
             'write.metadata.delete-after-commit.enabled' = 'true',
             'write.metadata.previous-versions-max'       = '10'
         );

   ── PASO 6 · Verificación física en S3
   --------------------------------------------------------------------------------------------
     Medir antes y después del mantenimiento:
       aws s3 ls s3://<bucket>/warehouse/telco_core/cdr_voz/ --recursive | wc -l
       aws s3 ls s3://<bucket>/warehouse/telco_core/cdr_voz/ --recursive \
         | awk '{s+=$3} END {print s/1024/1024/1024 " GB"}'

     ⚠ Estas consultas son solo de LECTURA/INVENTARIO. Nunca ejecutar aws s3 rm sobre
       prefijos de tablas Iceberg.

   ============================================================================================
   FIN DEL ESTÁNDAR
   ============================================================================================ */
