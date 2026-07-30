/* ============================================================================================
   05 · MANTENIMIENTO — SECUENCIA CANÓNICA
   Motor: Impala (Hue / Cloudera Data Explorer / impala-shell)
   ============================================================================================

   EL ORDEN NO ES NEGOCIABLE

     ① OPTIMIZE TABLE          → materializa delete files, compacta archivos pequeños
     ② EXPIRE_SNAPSHOTS        → elimina snapshots viejos y sus data files exclusivos
     ③ REMOVE_ORPHAN_FILES     → elimina archivos sin referencia en ningún snapshot
     ④ COMPUTE STATS           → refresca estadísticas del optimizador de Impala
     ⑤ INVALIDATE METADATA     → solo si el mantenimiento corrió desde CDE/Spark

   Saltarse ① en una tabla MoR hace que ② y ③ NO liberen espacio: los delete files siguen
   referenciados por el snapshot activo y por definición no son huérfanos.
   ============================================================================================ */


/* ============================================================================================
   ① OPTIMIZE TABLE — COMPACTACIÓN
   ============================================================================================
   Sintaxis oficial:  OPTIMIZE TABLE [db.]tabla [FILE_SIZE_THRESHOLD_MB=<valor>]

   Tareas que ejecuta:
     · Fusiona delete files con sus data files correspondientes (crítico en MoR)
     · Compacta data files menores al umbral especificado
     · Sin umbral: reescribe TODA la tabla al esquema y partition spec vigentes

   Requisitos:
     · write.format.default debe ser 'parquet'
     · El usuario necesita privilegio ALL sobre la tabla
     · No aplica sobre vistas

   Impala NO soporta filtro de partición en OPTIMIZE TABLE — reescribe toda la tabla.
   ============================================================================================ */

-- Mantenimiento recurrente — CON umbral (recomendado: ahorra cómputo)
-- Selecciona solo archivos pequeños y delete files; conserva los ya bien dimensionados.
OPTIMIZE TABLE telco_core.cdr_voz FILE_SIZE_THRESHOLD_MB=256;

-- Compactación completa — SIN umbral
-- Necesaria solo tras evolución de esquema o partition spec. Costosa en tablas grandes.
-- Con FILE_SIZE_THRESHOLD_MB, los archivos que no cumplen el umbral CONSERVAN el esquema
-- y layout de partición antiguos.
-- OPTIMIZE TABLE telco_core.cdr_voz;


/* ============================================================================================
   ② EXPIRE_SNAPSHOTS
   ============================================================================================
   Cuatro variantes soportadas (documentación Cloudera — Expire snapshots feature):
     ALTER TABLE <tbl> EXECUTE EXPIRE_SNAPSHOTS(<expresión timestamp>)
     ALTER TABLE <tbl> EXECUTE EXPIRE_SNAPSHOTS('<snapshot_id>')
     ALTER TABLE <tbl> EXECUTE EXPIRE_SNAPSHOTS('<id1>,<id2>,...')
     ALTER TABLE <tbl> EXECUTE EXPIRE_SNAPSHOTS BETWEEN (<ts1>) AND (<ts2>)
   ============================================================================================ */

-- Expresión relativa — PREFERIDA para jobs automatizados (no requiere parametrizar fecha)
ALTER TABLE telco_core.cdr_voz
    EXECUTE EXPIRE_SNAPSHOTS(NOW() - INTERVAL 7 DAYS);

-- Timestamp absoluto
-- ALTER TABLE telco_core.cdr_voz EXECUTE EXPIRE_SNAPSHOTS('2026-07-20 06:00:00');

-- Snapshot específico (p.ej. eliminar un snapshot con datos bajo solicitud GDPR)
-- ALTER TABLE telco_core.cdr_voz EXECUTE EXPIRE_SNAPSHOTS('3821459023847562930');

-- Rango entre dos timestamps
-- ALTER TABLE telco_core.cdr_voz
--     EXECUTE EXPIRE_SNAPSHOTS BETWEEN ('2026-06-01 00:00:00') AND ('2026-06-30 23:59:59');

/* PROTECCIÓN CONTRA EXPIRACIÓN EXCESIVA
   history.expire.min-snapshots-to-keep actúa como red de seguridad: aunque el timestamp
   cubra todos los snapshots, se conserva ese número mínimo.

   ⚠ ES UN CONTEO, NO UN DELTA DE TIEMPO. Documentación Cloudera: si la tabla recibe una
     modificación por hora, min=24 cubre 24 horas. Si recibe una por minuto, cubrir 24 horas
     requiere min=1440. Dimensionar según la frecuencia real de commits. */


/* ============================================================================================
   ③ REMOVE_ORPHAN_FILES
   ============================================================================================
   Elimina archivos presentes en el directorio de la tabla que no están referenciados por
   ningún snapshot.
   ============================================================================================ */

ALTER TABLE telco_core.cdr_voz
    EXECUTE REMOVE_ORPHAN_FILES(NOW() - INTERVAL 1 HOURS);

/* ⚠ EL TIMESTAMP ES LA CAUSA MÁS FRECUENTE DE FALLO SILENCIOSO

   REMOVE_ORPHAN_FILES('<ts>') solo elimina huérfanos CREADOS ANTES de <ts>. Los delete
   files que el OPTIMIZE acaba de dejar huérfanos tienen timestamp de creación reciente.

   Caso real de error:
     06:00  OPTIMIZE TABLE ...                              → deja delete files huérfanos
     06:30  EXECUTE EXPIRE_SNAPSHOTS('2026-07-27 00:00:00')
     06:31  EXECUTE REMOVE_ORPHAN_FILES('2026-07-27 00:00:00')
            → NO elimina nada: los huérfanos se crearon a las 06:00, después de las 00:00
            → Los delete files quedan acumulados en S3 indefinidamente

   Regla: el timestamp debe ser POSTERIOR al fin del OPTIMIZE.
   Usar expresiones relativas cortas elimina esta clase de error de raíz. */


/* ============================================================================================
   ④ ESTADÍSTICAS PARA EL OPTIMIZADOR DE IMPALA
   ============================================================================================
   Son DOS capas independientes:
     · Estadísticas de Iceberg (manifest files) → pruning de archivos. Automáticas.
     · Estadísticas de Impala (HMS)            → plan de ejecución, orden de joins,
                                                  estimación de cardinalidad. Manuales.

   El warning "missing relevant table and/or column statistics" se refiere a la SEGUNDA capa.
   Que Iceberg tenga estadísticas propias no lo elimina.

   ⚠ COMPUTE INCREMENTAL STATS NO ES FUNCIONAL EN ICEBERG.
     Impala lo convierte internamente en un COMPUTE STATS completo sobre toda la tabla.
   ============================================================================================ */

-- Tablas pequeñas y medianas: stats completas
COMPUTE STATS telco_core.dim_suscriptor;

-- Tablas de miles de millones de registros: sampling
-- Prerrequisito (una sola vez por tabla; es cambio de metadata, instantáneo,
-- independiente del tamaño de la tabla):
ALTER TABLE telco_core.cdr_voz
    SET TBLPROPERTIES ('impala.enable.stats.extrapolation'='true');

-- Sin la propiedad anterior:
--   AnalysisException: COMPUTE STATS TABLESAMPLE requires stats extrapolation
--   which is disabled.

-- Sampling al 10% — porcentajes muy por debajo de 10% dan resultados pobres.
-- Restringir a columnas que participan en filtros, joins y GROUP BY.
COMPUTE STATS telco_core.cdr_voz
    (fecha_evento, msisdn_origen, id_celda_origen, tipo_llamada, id_plan)
TABLESAMPLE SYSTEM(10);

/* Sobre REPEATABLE(<semilla>):
     COMPUTE STATS ... TABLESAMPLE SYSTEM(10) REPEATABLE(42);
   Fija la selección de archivos para que dos ejecuciones sean comparables.
   ⚠ En tablas con ingesta incremental continua, una semilla fija puede seleccionar siempre
     los mismos archivos históricos y excluir los nuevos. Para tablas CDR con carga diaria,
     OMITIR la semilla.

   Frecuencia de recálculo: cuando ~30% de las filas hayan cambiado. */

SHOW TABLE STATS telco_core.cdr_voz;
SHOW COLUMN STATS telco_core.cdr_voz;


/* ============================================================================================
   ⑤ SINCRONIZACIÓN DE CATÁLOGO
   ============================================================================================
   · Mantenimiento desde Impala/Hue  → NO se requiere. El Catalog Service de CDW propaga
                                       los cambios a todos los coordinadores automáticamente.
   · Mantenimiento desde CDE/Spark   → ejecutar INVALIDATE METADATA desde Impala.
                                       El HMS event polling sincroniza eventualmente, pero no
                                       garantiza inmediatez. Para un job nocturno seguido de
                                       lecturas al amanecer, el comando explícito es más seguro.

   Automatización desde un job CDE: ver cde/invalidate_metadata.py
   ============================================================================================ */

-- INVALIDATE METADATA telco_core.cdr_voz;


/* ============================================================================================
   SECUENCIA COMPLETA — PLANTILLA EJECUTABLE
   ============================================================================================ */

OPTIMIZE TABLE telco_core.cdr_voz FILE_SIZE_THRESHOLD_MB=256;

ALTER TABLE telco_core.cdr_voz
    EXECUTE EXPIRE_SNAPSHOTS(NOW() - INTERVAL 7 DAYS);

ALTER TABLE telco_core.cdr_voz
    EXECUTE REMOVE_ORPHAN_FILES(NOW() - INTERVAL 1 HOURS);

COMPUTE STATS telco_core.cdr_voz
    (fecha_evento, msisdn_origen, id_celda_origen, tipo_llamada, id_plan)
TABLESAMPLE SYSTEM(10);
