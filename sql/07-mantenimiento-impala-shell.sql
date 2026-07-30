/* ============================================================================================
   07 · MANTENIMIENTO PARAMETRIZADO PARA impala-shell
   ============================================================================================

   Este archivo es la versión ejecutable con `impala-shell -f`, equivalente a
   cde/maintenance_job.py pero sin dependencia de Spark ni de CDE.

   USO
   --------------------------------------------------------------------------------------------
   impala-shell \
       --protocol='hs2-http' --ssl \
       -i "<host-vw>:443" \
       -u "<usuario>" -l \
       --var=TABLA=telco_core.cdr_voz \
       --var=UMBRAL_MB=256 \
       --var=RETENCION_DIAS=7 \
       --var=COLUMNAS_STATS="fecha_evento, msisdn_origen, id_celda_origen" \
       -f sql/07-mantenimiento-impala-shell.sql

   Para procesar varias tablas, usar scripts/maintenance_impala.sh, que itera este archivo.

   NOTAS SOBRE LA SUSTITUCIÓN DE VARIABLES
   --------------------------------------------------------------------------------------------
   · Se declara en CLI como  --var=NOMBRE=valor
   · Se referencia en SQL como  ${var:NOMBRE}
   · La sustitución la hace impala-shell del lado cliente, no el backend impalad.
     Requiere impala-shell 2.5 o superior.
   · Si un comentario contiene ${...} y NO es una variable, hay que escapar el $  →  \${...}

   ============================================================================================ */


/* ── ESTADO INICIAL ────────────────────────────────────────────────────────────────────────
   Registrar el punto de partida para poder conciliar al final. */

SELECT '=== ESTADO INICIAL ===' AS fase;

SELECT COUNT(*) AS snapshots_activos
FROM ${var:TABLA}.snapshots;

SELECT
    content,
    COUNT(*)                                     AS archivos,
    SUM(record_count)                            AS registros,
    ROUND(SUM(file_size_in_bytes)/1024/1024, 2)  AS mb_total
FROM ${var:TABLA}.files
GROUP BY content
ORDER BY content;


/* ── ① COMPACTACIÓN ───────────────────────────────────────────────────────────────────────
   Materializa los delete files y compacta archivos pequeños.
   CRÍTICO en tablas merge-on-read: sin este paso, ② y ③ NO liberan espacio. */

SELECT '=== 1/4 · OPTIMIZE ===' AS fase;

OPTIMIZE TABLE ${var:TABLA} FILE_SIZE_THRESHOLD_MB=${var:UMBRAL_MB};


/* ── ② EXPIRACIÓN DE SNAPSHOTS ────────────────────────────────────────────────────────────
   La expresión relativa se evalúa del lado del servidor. Sintaxis confirmada en la
   documentación de Cloudera para Impala:
       ALTER TABLE ice_t EXECUTE EXPIRE_SNAPSHOTS(now() - interval 10 days);

   Ventaja sobre pasar un timestamp calculado externamente: no hay desfase de reloj entre
   el cliente y el cluster, ni riesgo de zona horaria. */

SELECT '=== 2/4 · EXPIRE_SNAPSHOTS ===' AS fase;

ALTER TABLE ${var:TABLA}
    EXECUTE EXPIRE_SNAPSHOTS(NOW() - INTERVAL ${var:RETENCION_DIAS} DAYS);


/* ── ③ ELIMINACIÓN DE HUÉRFANOS ───────────────────────────────────────────────────────────
   El timestamp DEBE ser posterior al momento en que terminó el OPTIMIZE del paso ①.
   Los delete files que el OPTIMIZE acaba de dejar huérfanos tienen timestamp de creación
   reciente; si el corte es anterior, quedan fuera de rango y NO se eliminan — sin que el
   comando reporte ningún error.

   Al ejecutarse en la misma sesión e inmediatamente después del OPTIMIZE, NOW() ya es
   posterior a él. El margen de 1 hora hacia atrás es holgura de seguridad.

   ⚠ VALIDAR EN EL AMBIENTE: la forma con expresión (NOW() - INTERVAL ...) está confirmada
     para EXPIRE_SNAPSHOTS en la documentación. Para REMOVE_ORPHAN_FILES no encontramos
     confirmación explícita de que acepte expresiones además de literales.
     Si esta sentencia falla con error de parseo, usar la variante con literal que está
     comentada abajo, y pasar el valor desde scripts/maintenance_impala.sh
     (que ya calcula ORPHAN_TS por si se necesita). */

SELECT '=== 3/4 · REMOVE_ORPHAN_FILES ===' AS fase;

ALTER TABLE ${var:TABLA}
    EXECUTE REMOVE_ORPHAN_FILES(NOW() - INTERVAL 1 HOURS);

-- VARIANTE CON LITERAL — usar si la de arriba falla con error de parseo:
-- ALTER TABLE ${var:TABLA}
--     EXECUTE REMOVE_ORPHAN_FILES('${var:ORPHAN_TS}');


/* ── ④ ESTADÍSTICAS ───────────────────────────────────────────────────────────────────────
   Solo se ejecuta si se pasó la variable COLUMNAS_STATS. Para omitir este paso,
   invocar el script con --var=COLUMNAS_STATS= (vacío) NO funciona: la sentencia quedaría
   inválida. Usar en su lugar la bandera --skip-stats de maintenance_impala.sh, que
   ejecuta una variante del script sin esta sección.

   Prerrequisito de TABLESAMPLE (aplicar una vez por tabla, ver 05-mantenimiento.sql):
       ALTER TABLE <tabla> SET TBLPROPERTIES ('impala.enable.stats.extrapolation'='true'); */

SELECT '=== 4/4 · COMPUTE STATS ===' AS fase;

COMPUTE STATS ${var:TABLA} (${var:COLUMNAS_STATS}) TABLESAMPLE SYSTEM(10);


/* ── ESTADO FINAL Y CONCILIACIÓN ──────────────────────────────────────────────────────────
   Comparar contra el estado inicial:
     · snapshots_activos debe haber disminuido
     · las filas con content IN (1,2) — delete files — deben haber desaparecido
       Si persisten, revisar docs/04-troubleshooting.md */

SELECT '=== ESTADO FINAL ===' AS fase;

SELECT COUNT(*) AS snapshots_activos
FROM ${var:TABLA}.snapshots;

SELECT
    content,
    COUNT(*)                                     AS archivos,
    SUM(record_count)                            AS registros,
    ROUND(SUM(file_size_in_bytes)/1024/1024, 2)  AS mb_total
FROM ${var:TABLA}.files
GROUP BY content
ORDER BY content;

SELECT
    CASE
        WHEN SUM(CASE WHEN content IN (1,2) THEN 1 ELSE 0 END) = 0
            THEN 'OK — sin delete files pendientes'
        ELSE 'REVISAR — quedan delete files: ver docs/04-troubleshooting.md'
    END AS resultado
FROM ${var:TABLA}.files;
