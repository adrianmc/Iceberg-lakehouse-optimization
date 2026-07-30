/* ============================================================================================
   06 · DIAGNÓSTICO Y VERIFICACIÓN
   Motor: Impala (Hue / Cloudera Data Explorer / impala-shell)
   ============================================================================================ */


/* 6.1 — Inventario de snapshots activos
   Si aparecen snapshots de fechas que se pretendía expirar, revisar
   history.expire.min-snapshots-to-keep (es un CONTEO, no un delta de tiempo). */
SELECT snapshot_id, committed_at, operation
FROM telco_core.cdr_voz.snapshots
ORDER BY committed_at DESC;


/* 6.2 — Acumulación de archivos por tipo
   content = 0 → data file
   content = 1 → position delete file
   content = 2 → equality delete file

   Si content 1 o 2 aparecen con volumen significativo, la tabla necesita OPTIMIZE.
   Estos archivos NO son huérfanos: están referenciados por el snapshot activo. */
SELECT
    content,
    COUNT(*)                                     AS archivos,
    SUM(record_count)                            AS registros,
    ROUND(SUM(file_size_in_bytes)/1024/1024, 2)  AS mb_total
FROM telco_core.cdr_voz.files
GROUP BY content
ORDER BY content;


/* 6.3 — Distribución de tamaño de data files (detecta el problema de small files) */
SELECT
    CASE
        WHEN file_size_in_bytes < 8388608   THEN '01 · < 8 MB'
        WHEN file_size_in_bytes < 33554432  THEN '02 · 8-32 MB'
        WHEN file_size_in_bytes < 134217728 THEN '03 · 32-128 MB'
        WHEN file_size_in_bytes < 268435456 THEN '04 · 128-256 MB'
        ELSE                                     '05 · > 256 MB'
    END                                               AS rango,
    COUNT(*)                                          AS archivos,
    ROUND(SUM(file_size_in_bytes)/1024/1024/1024, 2)  AS gb_total
FROM telco_core.cdr_voz.files
WHERE content = 0
GROUP BY 1
ORDER BY 1;


/* 6.4 — Historial de operaciones: identifica qué tipo de escritura domina la tabla
   Muchos 'delete' → la tabla acumula delete files, requiere OPTIMIZE frecuente
   Solo 'append'   → tabla append-only, el mantenimiento puede ser menos frecuente */
SELECT operation,
       COUNT(*)          AS veces,
       MIN(committed_at) AS primera,
       MAX(committed_at) AS ultima
FROM telco_core.cdr_voz.snapshots
GROUP BY operation
ORDER BY veces DESC;


/* 6.5 — Configuración de la tabla — EJECUTAR ANTES DE CUALQUIER DIAGNÓSTICO DE STORAGE
   Revisar en la salida:
     Table Type              → MANAGED_TABLE | EXTERNAL_TABLE
     external.table.purge    → debe ser 'true' en tablas EXTERNAL o no se borra nada de S3
     write.delete.mode       → merge-on-read si Impala escribe
     format-version          → 2
     history.expire.*        → política de retención vigente
     write.metadata.*        → limpieza de metadata habilitada */
DESCRIBE FORMATTED telco_core.cdr_voz;


/* 6.6 — Partition spec vigente */
SHOW PARTITIONS telco_core.cdr_voz;
SHOW CREATE TABLE telco_core.cdr_voz;


/* 6.7 — Historial completo y manifiestos */
SELECT * FROM telco_core.cdr_voz.history ORDER BY made_current_at DESC;
SELECT * FROM telco_core.cdr_voz.manifests;


/* 6.8 — Estadísticas del optimizador de Impala
   El warning "missing relevant table and/or column statistics" en el plan de ejecución
   se refiere a ESTAS estadísticas (capa HMS), no a las de Iceberg (manifest files). */
SHOW TABLE STATS telco_core.cdr_voz;
SHOW COLUMN STATS telco_core.cdr_voz;
