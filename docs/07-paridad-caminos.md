# Paridad entre los dos caminos de ejecución

Ambos caminos se mantienen deliberadamente. No son redundantes: **son complementarios**, y
cada uno cubre un modo de fallo del otro.

| | **impala-shell** | **CDE / Spark** |
|---|---|---|
| Punto de entrada | `scripts/maintenance_impala.sh` | `cde/maintenance_job.py` |
| Dependencias | `impala-shell` | PySpark + `impyla` |
| Falla si… | El VW de Impala no responde | CDE no responde, o hay bloqueo de red hacia el cluster |

---

## Los comandos NO son intercambiables

Este es el punto que hace que ambos caminos deban existir por separado en lugar de ser un
solo script parametrizado.

| Operación | Impala / Hive | Spark / CDE |
|---|---|---|
| Compactar | `OPTIMIZE TABLE t FILE_SIZE_THRESHOLD_MB=n` | `CALL cat.system.rewrite_data_files(table => 't')` |
| Dangling deletes | *(cubierto por `OPTIMIZE TABLE`)* | `CALL cat.system.rewrite_position_delete_files(table => 't')` |
| Expirar snapshots | `ALTER TABLE t EXECUTE EXPIRE_SNAPSHOTS(...)` | `CALL cat.system.expire_snapshots(table => 't', ...)` |
| Huérfanos | `ALTER TABLE t EXECUTE REMOVE_ORPHAN_FILES(...)` | `CALL cat.system.remove_orphan_files(table => 't', ...)` |
| Estadísticas | `COMPUTE STATS t (...) TABLESAMPLE SYSTEM(10)` | **No existe** |
| Sincronizar catálogo | *(no requerido)* | **No existe** — es comando de Impala |

> Ejecutar `spark.sql("OPTIMIZE TABLE ...")` **falla**. Ejecutar
> `CALL system.expire_snapshots(...)` desde Impala **falla**. Es el mismo error que este
> repositorio documenta como causa raíz: aplicar la sintaxis de un motor a otro.

---

## Asimetría 1 — el `OPTIMIZE` de Impala hace dos cosas

`OPTIMIZE TABLE` de Impala compacta **y** resuelve los position deletes en una sola
operación. En Spark eso requiere dos procedimientos, y omitir el segundo reproduce
exactamente el síntoma que originó este estándar.

Documentación de Apache Iceberg sobre `rewrite_position_delete_files`:

> *"After rewrite_data_files, position delete records pointing to the rewritten data files
> are not always marked for removal, and can remain tracked by the table's live snapshot
> metadata. This is known as the 'dangling delete' problem."*

**Consecuencia práctica:** un job de Spark que solo llame a `rewrite_data_files` y luego
expire snapshots dejará delete files vivos en S3 — sin reportar ningún error. Por eso
`maintenance_job.py` incluye el paso 2 de forma obligatoria.

---

## Asimetría 2 — el camino por CDE siempre requiere dos pasos

`COMPUTE STATS` e `INVALIDATE METADATA` son comandos de Impala. Spark no los tiene.

```
Camino impala-shell:   [1 paso]   maintenance_impala.sh          → completo
Camino CDE:            [2 pasos]  maintenance_job.py (Spark)
                                  invalidate_metadata.py (Impala) → completo
                                  + COMPUTE STATS manual o vía impala-shell
```

Esto no es un defecto del diseño del job: es una propiedad del stack. Vale la pena tenerlo
presente al estimar la ventana de mantenimiento.

---

## Configuración compartida

Ambos caminos leen **los mismos nombres de variable**, para que los parámetros no diverjan:

| Variable | `maintenance_impala.sh` | `maintenance_job.py` |
|---|:--:|:--:|
| `TABLAS` | ✔ | ✔ |
| `RETENCION_DIAS` | ✔ | ✔ |
| `UMBRAL_MB` | ✔ | ✔ |
| `COLUMNAS_STATS` | ✔ | — (Spark no tiene `COMPUTE STATS`) |
| `RETAIN_LAST` | — (implícito en Impala) | ✔ |
| `SPARK_CATALOG` | — | ✔ |
| `WHERE_FILTER` | — (Impala no filtra en `OPTIMIZE`) | ✔ |

El mismo `scripts/config.env` sirve para alimentar las variables de entorno del job de CDE.

---

## Cuándo usar cada uno

| Situación | Camino |
|---|---|
| Mantenimiento estándar sobre tablas MoR | **impala-shell** — un solo paso |
| Conectividad hacia CDE degradada o bloqueada | **impala-shell** |
| No hay `impala-shell` instalado en el host de automatización | **CDE** |
| Compactación filtrada por partición | **CDE** — Impala no soporta filtro en `OPTIMIZE TABLE` |
| Tablas copy-on-write | **CDE o Hive** — Impala no escribe CoW |
| Mantenimiento embebido en un pipeline Spark mayor | **CDE** |
| Tabla muy grande donde la compactación necesita paralelismo del cluster | **CDE** |
| Orquestación con dependencias complejas entre jobs | **CDE** o scheduler empresarial |

---

## Procedimiento de failover

Si el camino primario falla, el secundario debe poder ejecutarse **sin preparación previa**.
De eso depende que la redundancia sirva de algo.

### Si falla CDE

```bash
# 1. Confirmar que es problema de CDE y no de las tablas
#    (revisar el log del job: errores de red vs errores de SQL)

# 2. Ejecutar el camino alterno desde cualquier host con impala-shell
./scripts/maintenance_impala.sh --tablas "telco_core.cdr_voz,telco_core.cdr_datos"

# 3. No se requiere INVALIDATE METADATA: corrió desde Impala
```

### Si falla el VW de Impala

```bash
# 1. Ejecutar el job de Spark en CDE
export TABLAS="telco_core.cdr_voz,telco_core.cdr_datos"
export RETENCION_DIAS=7
export UMBRAL_MB=256
# (lanzar maintenance_job.py como job de CDE)

# 2. Cuando el VW de Impala se restablezca, ejecutar los pasos pendientes:
python cde/invalidate_metadata.py

impala-shell --protocol='hs2-http' --ssl -i "<host>:443" -u "<usuario>" -l \
    -q 'COMPUTE STATS telco_core.cdr_voz (fecha_evento, msisdn_origen) TABLESAMPLE SYSTEM(10)'
```

### Verificación posterior, independiente del camino usado

```sql
-- Debe retornar cero filas. Si retorna algo, ver docs/04-troubleshooting.md
SELECT content, COUNT(*) AS archivos
FROM telco_core.cdr_voz.files
WHERE content IN (1, 2)
GROUP BY content;
```

```bash
./scripts/s3_inventory.sh s3://<bucket>/warehouse/telco_core/cdr_voz post_mantenimiento
```

---

## Mantener la paridad en el tiempo

Si se modifica la secuencia en un camino, **debe reflejarse en el otro**. El riesgo real de
mantener dos implementaciones es que diverjan silenciosamente.

Checklist al modificar cualquiera de los dos:

- [ ] ¿El cambio aplica también al otro camino?
- [ ] Si aplica, ¿se implementó con la **sintaxis correcta de ese motor**?
- [ ] ¿Los nombres de variable de configuración siguen coincidiendo?
- [ ] ¿Se actualizó la tabla de equivalencias de este documento?
- [ ] ¿Se probó `--dry-run` en ambos?

El `PULL_REQUEST_TEMPLATE.md` incluye este checklist.
