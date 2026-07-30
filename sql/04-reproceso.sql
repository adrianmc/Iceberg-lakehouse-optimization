/* ============================================================================================
   04 · REPROCESO DE PARTICIONES Y ARCHIVADO FRÍO
   Motor: Impala (Hue / Cloudera Data Explorer / impala-shell)
   ============================================================================================ */


/* ============================================================================================
   4.1 — PATRÓN RECOMENDADO: DROP PARTITION + INSERT
   ============================================================================================
   No genera delete files. Es el patrón correcto para reprocesos durante ventana de lectura,
   porque las consultas concurrentes no pagan overhead de merge-on-read.
   ============================================================================================ */

-- Paso 1 · Validar el volumen del origen ANTES de destruir el destino
SELECT fecha_evento, COUNT(*) AS registros
FROM telco_staging.stg_cdr_voz_reproceso
WHERE fecha_evento BETWEEN DATE '2026-06-01' AND DATE '2026-06-03'
GROUP BY fecha_evento
ORDER BY fecha_evento;

-- Paso 2 · Registrar el conteo actual del destino (conciliación posterior)
SELECT fecha_evento, COUNT(*) AS registros_antes
FROM telco_core.cdr_voz
WHERE fecha_evento BETWEEN DATE '2026-06-01' AND DATE '2026-06-03'
GROUP BY fecha_evento
ORDER BY fecha_evento;

-- Paso 3 · Eliminar las particiones a reprocesar
ALTER TABLE telco_core.cdr_voz
    DROP PARTITION (DAYS(fecha_evento) >= '2026-06-01'
                AND DAYS(fecha_evento) <= '2026-06-03');

-- Paso 4 · Recargar
-- Iceberg usa hidden partitioning: NO se declara la partición en el INSERT
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

-- Paso 6 · Liberar S3
-- Opcional inmediato si las particiones son grandes; si no, el mantenimiento nocturno lo cubre
ALTER TABLE telco_core.cdr_voz
    EXECUTE EXPIRE_SNAPSHOTS(NOW() - INTERVAL 1 HOURS);

/* NOTA: OPTIMIZE no es necesario en este flujo. DROP PARTITION + INSERT no genera
   delete files — no hay nada que materializar. */


/* ============================================================================================
   4.2 — REPROCESO CUANDO DROP PARTITION NO APLICA
   ============================================================================================
   Si el criterio de reproceso no coincide con límites de partición, el DELETE es inevitable.
   En ese caso, OPTIMIZE inmediatamente después para no arrastrar delete files durante toda
   la ventana de lectura.
   ============================================================================================ */

DELETE FROM telco_core.cdr_voz
WHERE fecha_evento = DATE '2026-06-15'
  AND id_operador_destino = 'OPERADOR_X';

INSERT INTO telco_core.cdr_voz
SELECT * FROM telco_staging.stg_cdr_voz_reproceso
WHERE fecha_evento = DATE '2026-06-15'
  AND id_operador_destino = 'OPERADOR_X';

-- OBLIGATORIO tras un DELETE durante ventana de lectura
OPTIMIZE TABLE telco_core.cdr_voz FILE_SIZE_THRESHOLD_MB=256;


/* ============================================================================================
   4.3 — ARCHIVADO FRÍO: copy-out → prune → expire
   ============================================================================================
   Único patrón seguro para mover histórico a almacenamiento de bajo costo sin tocar S3
   manualmente.
   ============================================================================================ */

-- Paso 1 · Copiar a la tabla de archivo (prefijo S3 desacoplado)
INSERT INTO telco_archive.cdr_voz_historico
SELECT * FROM telco_core.cdr_voz
WHERE fecha_evento < DATE '2024-01-01';

-- Paso 2 · Conciliar ANTES de destruir el origen
SELECT
    (SELECT COUNT(*) FROM telco_core.cdr_voz
      WHERE fecha_evento < DATE '2024-01-01')      AS origen,
    (SELECT COUNT(*) FROM telco_archive.cdr_voz_historico
      WHERE fecha_evento < DATE '2024-01-01')      AS destino;

-- Paso 3 · Eliminar del operativo (SOLO si el conteo cuadra)
ALTER TABLE telco_core.cdr_voz
    DROP PARTITION (DAYS(fecha_evento) < '2024-01-01');

-- Paso 4 · Expirar snapshots — SIN ESTE PASO S3 NO LIBERA NADA
ALTER TABLE telco_core.cdr_voz
    EXECUTE EXPIRE_SNAPSHOTS(NOW() - INTERVAL 1 HOURS);

-- Paso 5 · Limpiar huérfanos
ALTER TABLE telco_core.cdr_voz
    EXECUTE REMOVE_ORPHAN_FILES(NOW() - INTERVAL 1 HOURS);

/* ⚠ S3 LIFECYCLE RULES
   Aplicar ÚNICAMENTE sobre s3://<bucket-archive>/warehouse/telco_archive/
   NUNCA sobre el prefijo operativo. Los archivos del operativo son gestionados
   exclusivamente por el garbage collector de Iceberg. */
