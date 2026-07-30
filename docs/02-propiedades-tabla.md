# Catálogo de propiedades de tabla

Referencia de `TBLPROPERTIES` relevantes para Iceberg v2 en Cloudera CDW.

---

## Formato

| Propiedad | Valor | Notas |
|---|---|---|
| `format-version` | `'2'` | Obligatorio para row-level deletes, MoR y time travel completo |
| `write.format.default` | `'parquet'` | **Requerido por `OPTIMIZE TABLE` en Impala.** Sin esto la compactación falla |
| `write.parquet.compression-codec` | `'zstd'` | Mejor ratio que snappy para CDR. Reduce storage y egress |
| `write.target-file-size-bytes` | `'268435456'` | 256 MB para CDR de alto volumen. 128 MB para streaming |

---

## Modo de escritura

| Propiedad | Valores |
|---|---|
| `write.delete.mode` | `merge-on-read` \| `copy-on-write` |
| `write.update.mode` | `merge-on-read` \| `copy-on-write` |
| `write.merge.mode` | `merge-on-read` \| `copy-on-write` |

> `copy-on-write` hace **fallar** toda escritura desde Impala.

Se pueden mezclar por tipo de operación. Ejemplo útil: `delete`/`update` en MoR (borrados
puntuales rápidos) combinado con `merge` en CoW (upserts masivos sin generar miles de delete
files). Ese patrón solo es válido si todas las escrituras corren en Spark/CDE o Hive.

---

## Retención de snapshots

| Propiedad | Descripción |
|---|---|
| `history.expire.max-snapshot-age-ms` | Edad máxima antes de ser elegible para expiración |
| `history.expire.min-snapshots-to-keep` | **Conteo** de snapshots a retener, no delta de tiempo |

Valores de referencia para `max-snapshot-age-ms`:

| Milisegundos | Equivalente |
|---|---|
| `86400000` | 1 día |
| `604800000` | 7 días — recomendado para tablas operativas |
| `2592000000` | 30 días |

### Dimensionamiento de `min-snapshots-to-keep`

Documentación Cloudera: si la tabla recibe una modificación por hora, `24` cubre 24 horas.
Si recibe una por minuto, cubrir 24 horas requiere `1440`.

| Patrón de escritura | Valor sugerido |
|---|---|
| CDE inserta ~1 vez/minuto durante 6 h (≈360 commits/noche) | `400` |
| Carga diaria única | `30` |
| Solo un punto de rollback | `5` |

---

## Limpieza de metadata

| Propiedad | Valor | Efecto |
|---|---|---|
| `write.metadata.delete-after-commit.enabled` | `'true'` | Sin esto los metadata files **no se eliminan** aunque los snapshots expiren |
| `write.metadata.previous-versions-max` | `'10'` | Cuántas versiones de metadata conservar |

> Causa frecuente de crecimiento del prefijo `/metadata/`: estas dos propiedades ausentes.

---

## Tablas externas

| Propiedad | Valor | Efecto |
|---|---|---|
| `external.table.purge` | `'true'` | Habilita que Iceberg elimine físicamente archivos de S3 |

Sin esta propiedad en una tabla `EXTERNAL_TABLE`, `EXPIRE_SNAPSHOTS` y `REMOVE_ORPHAN_FILES`
limpian el catálogo pero **dejan todos los parquets en el bucket**.

---

## Estadísticas de Impala

| Propiedad | Valor | Efecto |
|---|---|---|
| `impala.enable.stats.extrapolation` | `'true'` | Requerido para `COMPUTE STATS ... TABLESAMPLE` |

Sin esta propiedad:

```
AnalysisException: COMPUTE STATS TABLESAMPLE requires stats extrapolation which is
disabled. Stats extrapolation can be enabled service-wide with
--enable_stats_extrapolation=true or by altering the table to have tblproperty
impala.enable.stats.extrapolation=true
```

Habilitarlo a nivel de tabla es un cambio de metadata: **instantáneo, independiente del
tamaño de la tabla**. La alternativa a nivel de servicio requiere acceso a la configuración
del Virtual Warehouse.

---

## Aplicar propiedades a una tabla existente

```sql
ALTER TABLE <db>.<tabla> SET TBLPROPERTIES (
    'history.expire.max-snapshot-age-ms'         = '604800000',
    'history.expire.min-snapshots-to-keep'       = '400',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max'       = '10',
    'impala.enable.stats.extrapolation'          = 'true'
);
```

Verificar:

```sql
DESCRIBE FORMATTED <db>.<tabla>;
```
