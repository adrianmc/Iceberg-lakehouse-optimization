# Troubleshooting — "El mantenimiento corrió pero S3 no libera espacio"

Ejecutar en este orden. Detenerse en el primer hallazgo positivo.

---

## Paso 1 · ¿La tabla es EXTERNAL sin purge habilitado?

```sql
DESCRIBE FORMATTED <db>.<tabla>;
```

Buscar:

```
Table Type:             EXTERNAL_TABLE
external.table.purge    (ausente o 'false')
```

**Si coincide → causa confirmada.** En tablas externas sin `external.table.purge='true'`,
Iceberg no elimina los archivos físicos de S3. Los snapshots desaparecen del catálogo pero
los parquets permanecen acumulando costo.

```sql
ALTER TABLE <db>.<tabla> SET TBLPROPERTIES ('external.table.purge'='true');
```

Luego re-ejecutar la secuencia completa de mantenimiento.

---

## Paso 2 · ¿Quedan delete files activos? (tablas MoR)

```sql
SELECT content, COUNT(*) AS archivos, SUM(record_count) AS registros
FROM <db>.<tabla>.files
WHERE content IN (1, 2)
GROUP BY content;
```

**Si retorna filas →** los delete files están **referenciados por el snapshot activo**. No
son huérfanos, y `REMOVE_ORPHAN_FILES` nunca los tocará por diseño.

Corrección:

```sql
OPTIMIZE TABLE <db>.<tabla> FILE_SIZE_THRESHOLD_MB=256;
ALTER TABLE <db>.<tabla> EXECUTE EXPIRE_SNAPSHOTS(NOW() - INTERVAL 7 DAYS);
ALTER TABLE <db>.<tabla> EXECUTE REMOVE_ORPHAN_FILES(NOW() - INTERVAL 1 HOURS);
```

---

## Paso 3 · ¿El timestamp de `REMOVE_ORPHAN_FILES` fue anterior al `OPTIMIZE`?

Comparar la hora real de ejecución del `OPTIMIZE` contra el timestamp pasado al comando.

**Si el timestamp es anterior →** los huérfanos recién creados quedaron fuera de rango y no
se eliminaron.

```sql
ALTER TABLE <db>.<tabla> EXECUTE REMOVE_ORPHAN_FILES(NOW() - INTERVAL 1 HOURS);
```

Este es el error más frecuente y el más difícil de detectar, porque **el comando no falla**:
simplemente no encuentra nada que eliminar dentro del rango indicado.

---

## Paso 4 · ¿Los snapshots realmente expiraron?

```sql
SELECT snapshot_id, committed_at, operation
FROM <db>.<tabla>.snapshots
ORDER BY committed_at ASC;
```

**Si aún aparecen snapshots del período que se pretendía expirar →** revisar
`history.expire.min-snapshots-to-keep`.

Recordar que es un **conteo, no un delta de tiempo**. Un valor alto en una tabla con commits
frecuentes retiene mucho más tiempo del esperado. Ejemplo: `min-snapshots-to-keep=1000` en
una tabla con 360 commits por noche retiene casi tres días de snapshots aunque el timestamp
de expiración indique uno.

---

## Paso 5 · ¿Los archivos remanentes son metadata, no data?

Revisar si los objetos remanentes están bajo el prefijo `/metadata/` (extensiones `.avro`,
`.json`).

> Documentación Cloudera — *Expire snapshots feature*:
> *"Expiring a snapshot does not remove old metadata files by default."*

```sql
ALTER TABLE <db>.<tabla> SET TBLPROPERTIES (
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max'       = '10'
);
```

Estas propiedades controlan la eliminación automática de metadata files tras operaciones
como expirar snapshots o insertar datos.

---

## Paso 6 · Verificación física en S3

```bash
./scripts/s3_inventory.sh s3://<bucket>/warehouse/<db>/<tabla> antes
# ... ejecutar mantenimiento ...
./scripts/s3_inventory.sh s3://<bucket>/warehouse/<db>/<tabla> despues
diff inventario_antes.txt inventario_despues.txt
```

> Estas operaciones son **solo de lectura/inventario**. Nunca ejecutar `aws s3 rm` sobre
> prefijos de tablas Iceberg: rompe el catálogo de forma potencialmente irrecuperable.

---

## Árbol de decisión resumido

```
¿S3 no libera espacio?
│
├─ ¿Tabla EXTERNAL sin purge? ────────────► ALTER TABLE ... external.table.purge='true'
│
├─ ¿Quedan delete files (content 1|2)? ───► OPTIMIZE antes de EXPIRE_SNAPSHOTS
│
├─ ¿Timestamp de ORPHAN < hora OPTIMIZE? ─► Re-ejecutar con NOW() - INTERVAL 1 HOURS
│
├─ ¿Snapshots viejos siguen activos? ─────► Revisar min-snapshots-to-keep (es un CONTEO)
│
└─ ¿Archivos bajo /metadata/? ────────────► write.metadata.delete-after-commit.enabled=true
```
