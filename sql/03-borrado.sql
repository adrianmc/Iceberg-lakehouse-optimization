/* ============================================================================================
   03 · BORRADO DE DATOS
   Motor: Impala (Hue / Cloudera Data Explorer / impala-shell)
   ============================================================================================

   REGLA FUNDAMENTAL
   --------------------------------------------------------------------------------------------
   Ningún comando de borrado libera espacio en S3 por sí solo. DELETE, UPDATE, MERGE,
   INSERT OVERWRITE y ALTER TABLE DROP PARTITION crean un nuevo snapshot; los data files
   del snapshot anterior permanecen físicamente en S3 hasta que ese snapshot expire.

   La liberación física SIEMPRE requiere EXPIRE_SNAPSHOTS posterior (ver 05-mantenimiento.sql).
   ============================================================================================ */


/* --------------------------------------------------------------------------------------------
   3.1 — MÉTODO PREFERIDO: ALTER TABLE DROP PARTITION
   --------------------------------------------------------------------------------------------
   Ventaja decisiva en tablas MoR: NO genera delete files. Elimina la partición a nivel de
   snapshot. Las lecturas concurrentes no pagan overhead de merge-on-read.

   REGLAS DEL FILTRO (documentación Cloudera — Drop partition feature):
     · Debe ser combinación de, o al menos uno de: predicado binario, IN, IS NULL
     · El argumento del predicado DEBE ser un partition transform:
       DAYS(col), MONTHS(col), YEARS(col), BUCKET(n,col), TRUNCATE(n,col) — o columna identity
     · Todo transform no-identity debe aparecer en el filtro tal como está en el partition spec.
       Si la tabla se particiona por DAYS(fecha), el filtro debe usar DAYS(fecha), no fecha.
     · Operadores soportados: =, !=, <, >, <=, >=
   -------------------------------------------------------------------------------------------- */

-- Una partición específica
ALTER TABLE telco_core.cdr_voz
    DROP PARTITION (DAYS(fecha_evento) = '2026-06-15');

-- Todo lo anterior a una fecha
ALTER TABLE telco_core.cdr_voz
    DROP PARTITION (DAYS(fecha_evento) < '2026-01-01');

-- Rango acotado — dos predicados binarios combinados
ALTER TABLE telco_core.cdr_voz
    DROP PARTITION (DAYS(fecha_evento) >= '2026-06-01'
                AND DAYS(fecha_evento) <= '2026-06-30');

-- Partición mensual
ALTER TABLE telco_core.agregados_facturacion
    DROP PARTITION (MONTHS(fecha_corte) = '2026-03');

-- Predicado IN
ALTER TABLE telco_core.cdr_voz
    DROP PARTITION (DAYS(fecha_evento) IN ('2026-06-01', '2026-06-02', '2026-06-03'));


/* --------------------------------------------------------------------------------------------
   3.2 — MÉTODO ALTERNATIVO: DELETE
   --------------------------------------------------------------------------------------------
   Usar ÚNICAMENTE cuando el criterio de borrado NO coincide con límites de partición.
   -------------------------------------------------------------------------------------------- */

-- Criterio que cruza particiones parcialmente
DELETE FROM telco_core.cdr_voz
WHERE codigo_terminacion = 'ERR_TIMEOUT'
  AND fecha_evento BETWEEN DATE '2026-06-01' AND DATE '2026-06-30';

-- Criterio de negocio no alineado a partición (GDPR / derecho al olvido)
DELETE FROM telco_core.cdr_voz
WHERE msisdn_origen = '<msisdn_solicitante>';

/* CONSECUENCIAS DEL DELETE EN MoR:
     · Genera position delete files inmediatamente
     · TODAS las lecturas concurrentes deben aplicarlos en tiempo de lectura
     · Los delete files NO desaparecen con EXPIRE_SNAPSHOTS ni REMOVE_ORPHAN_FILES:
       están referenciados por el snapshot activo, por lo tanto NO son huérfanos
     · Solo OPTIMIZE TABLE los materializa y los convierte en huérfanos elegibles

   Si un DELETE corre durante la ventana de lectura diurna, el overhead persiste hasta el
   siguiente OPTIMIZE — potencialmente 18-20 horas.

   Mitigación obligatoria si el DELETE corre en horario de lectura: */
OPTIMIZE TABLE telco_core.cdr_voz FILE_SIZE_THRESHOLD_MB=256;


/* --------------------------------------------------------------------------------------------
   3.3 — ANTIPATRONES
   --------------------------------------------------------------------------------------------

   ✘ Borrar objetos directamente en S3 (consola AWS, aws s3 rm, scripts)
       → Rompe el catálogo. Snapshots apuntan a archivos inexistentes.
         Daño potencialmente irrecuperable. SIN EXCEPCIONES.

   ✘ DELETE cuando el criterio coincide exactamente con una partición
       → Genera delete files innecesarios. Usar DROP PARTITION.

   ✘ INSERT OVERWRITE asumiendo semántica Hive
       → En Iceberg NO reemplaza archivos físicos. Crea un snapshot nuevo y deja los
         anteriores como huérfanos pendientes de expiración.

   ✘ DROP PARTITION o DELETE sin EXPIRE_SNAPSHOTS posterior
       → El espacio en S3 nunca se libera. Crecimiento silencioso de costo.

   ✘ EXPIRE_SNAPSHOTS en tabla MoR sin OPTIMIZE previo
       → Los delete files sobreviven. El espacio no se libera.

   ✘ DELETE / UPDATE desde Impala sobre tabla en copy-on-write
       → La operación FALLA. Impala solo escribe en merge-on-read.

   -------------------------------------------------------------------------------------------- */
