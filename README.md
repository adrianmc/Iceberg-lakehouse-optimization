# Estándar Iceberg v2 — Telco / Cloudera CDW

> Guía operativa y scripts validados para diseño, retención, borrado y mantenimiento de tablas
> **Apache Iceberg v2** en **Cloudera Data Warehouse** (Impala / Hive / CDE).
> Perfil de referencia: operadores de telecomunicaciones (CDR, eventos de red, sesiones de datos).

![Estado](https://img.shields.io/badge/estado-activo-brightgreen)
![Motor](https://img.shields.io/badge/motor-Impala%20%7C%20Hive%20%7C%20CDE-blue)
![Iceberg](https://img.shields.io/badge/Iceberg-v2-orange)

---

## Qué resuelve este asset

Los patrones de gestión de datos heredados de Hive **no son seguros en Iceberg v2**. Este
repositorio documenta y corrige los errores más costosos que aparecen en campo:

| Síntoma reportado por el cliente | Causa raíz real |
|---|---|
| "Borramos particiones pero S3 no baja de tamaño" | Falta `EXPIRE_SNAPSHOTS` posterior |
| "Ejecutamos mantenimiento y quedan parquets `delete-`" | Falta `OPTIMIZE` antes de expirar, o el timestamp de `REMOVE_ORPHAN_FILES` es anterior al `OPTIMIZE` |
| "Configuramos copy-on-write y las escrituras fallan" | **Impala solo escribe en Merge-on-Read** |
| "Borramos los archivos desde la consola de S3" | Catálogo corrupto — snapshots apuntan a archivos inexistentes |

---

## Hecho que condiciona todo el diseño

> **Impala soporta únicamente el modo MERGE-ON-READ para escritura.**
>
> Documentación Cloudera — *Row-level operations*:
> *"Impala supports only the MOR mode and will fail if configured for copy-on-write.
> Impala does support reading copy-on-write tables."*

Consecuencia: el modo de escritura **no se elige por patrón de uso en abstracto**, sino
primero por **qué motor ejecuta los `DELETE` / `UPDATE` / `MERGE`**.

| Motor que escribe | Modos disponibles |
|---|---|
| Solo Impala | MoR obligatorio |
| Hive | MoR o CoW |
| Spark / CDE | MoR o CoW |
| Mixto (Impala + Spark) | MoR obligatorio |

---

## Secuencia canónica de mantenimiento

El orden **no es negociable**:

```
① OPTIMIZE TABLE          → materializa delete files, compacta archivos pequeños
② EXPIRE_SNAPSHOTS        → elimina snapshots viejos y sus data files exclusivos
③ REMOVE_ORPHAN_FILES     → elimina archivos sin referencia en ningún snapshot
④ COMPUTE STATS           → refresca estadísticas del optimizador de Impala
⑤ INVALIDATE METADATA     → solo si el mantenimiento corrió desde CDE/Spark
```

Saltarse ① en una tabla MoR hace que ② y ③ **no liberen espacio**: los delete files siguen
referenciados por el snapshot activo y por definición no son huérfanos.

---

## Dos caminos de ejecución — ambos mantenidos

El mantenimiento se puede ejecutar por cualquiera de las dos vías. **Se mantienen ambas
deliberadamente**: no son redundantes, cada una cubre un modo de fallo de la otra.

| | **impala-shell** | **CDE / Spark** |
|---|---|---|
| Punto de entrada | `scripts/maintenance_impala.sh` | `cde/maintenance_job.py` |
| Dependencias | `impala-shell` | PySpark + `impyla` |
| Pasos para completar | **1** | **2** (Spark + seguimiento en Impala) |
| `INVALIDATE METADATA` | No requerido | Requerido |
| `COMPUTE STATS` | Incluido | No existe en Spark |
| Compactación filtrada por partición | No soportado | Sí |
| Tablas copy-on-write | No — Impala no escribe CoW | Sí |
| Falla si… | El VW de Impala no responde | CDE no responde o hay bloqueo de red |

### Los comandos no son intercambiables

| Operación | Impala / Hive | Spark / CDE |
|---|---|---|
| Compactar | `OPTIMIZE TABLE t FILE_SIZE_THRESHOLD_MB=n` | `CALL cat.system.rewrite_data_files(...)` |
| Dangling deletes | *(cubierto por `OPTIMIZE TABLE`)* | `CALL cat.system.rewrite_position_delete_files(...)` |
| Expirar snapshots | `ALTER TABLE t EXECUTE EXPIRE_SNAPSHOTS(...)` | `CALL cat.system.expire_snapshots(...)` |
| Huérfanos | `ALTER TABLE t EXECUTE REMOVE_ORPHAN_FILES(...)` | `CALL cat.system.remove_orphan_files(...)` |

> Ejecutar `spark.sql("OPTIMIZE TABLE ...")` **falla**. Es el mismo error que este estándar
> documenta como causa raíz: aplicar la sintaxis de un motor a otro.

Ambos caminos leen los mismos nombres de variable desde `scripts/config.env`, para que los
parámetros no diverjan. Paridad completa y procedimiento de failover en
[`docs/07-paridad-caminos.md`](docs/07-paridad-caminos.md).

---

## Estructura del repositorio

```
.
├── sql/
│   ├── 00-estandar-completo.sql      Documento maestro — referencia integral comentada
│   ├── 01-setup-databases.sql        Creación de bases y separación de prefijos S3
│   ├── 02-ddl-tablas.sql             DDL: MoR, CoW, mixto, externa, staging, archivo
│   ├── 03-borrado.sql                DROP PARTITION vs DELETE + antipatrones
│   ├── 04-reproceso.sql              Reproceso de particiones y archivado frío
│   ├── 05-mantenimiento.sql          Secuencia canónica parametrizable
│   ├── 06-diagnostico.sql            Queries de inspección y verificación
│   └── 07-mantenimiento-impala-shell.sql   Versión parametrizada para `impala-shell -f`
├── cde/
│   ├── maintenance_job.py            Job PySpark de mantenimiento recurrente
│   ├── invalidate_metadata.py        Sincronización de catálogo vía impyla (LDAP)
│   └── requirements.txt
├── scripts/
│   ├── maintenance_impala.sh         Orquestador vía impala-shell (sin Spark ni CDE)
│   ├── config.env.example            Plantilla de configuración
│   └── s3_inventory.sh               Inventario S3 antes/después (solo lectura)
├── docs/
│   ├── 01-decision-mor-cow.md        Árbol de decisión del modo de escritura
│   ├── 02-propiedades-tabla.md       Catálogo de TBLPROPERTIES
│   ├── 03-runbook-mantenimiento.md   Calendario y disparadores
│   ├── 04-troubleshooting.md         "El mantenimiento corrió pero S3 no libera"
│   ├── 05-proteccion-repositorio.md  Configuración de permisos y rulesets
│   ├── 06-ejecucion-impala-shell.md  Camino vía impala-shell: instalación y uso
│   └── 07-paridad-caminos.md         Equivalencias, asimetrías y failover entre caminos
└── .github/
    ├── CODEOWNERS
    ├── PULL_REQUEST_TEMPLATE.md
    ├── ISSUE_TEMPLATE/
    └── workflows/validate.yml
```

---

## Uso rápido

### 1 · Descargar

```bash
# Clonar
git clone https://github.com/<owner>/<repo>.git

# O descargar la rama principal como ZIP
curl -L -o iceberg-telco.zip \
  https://github.com/<owner>/<repo>/archive/refs/heads/main.zip
```

### 2 · Adaptar los placeholders

| Placeholder | Reemplazar por |
|---|---|
| `<bucket-datalake>` | Bucket S3 del data lake operativo |
| `<bucket-archive>` | Bucket S3 de archivo (prefijo **desacoplado**) |
| `telco_core` / `telco_staging` / `telco_archive` | Nombres reales de las bases |
| `cdr_voz` / `cdr_datos` / `dim_suscriptor` | Nombres reales de las tablas |

### 3 · Validar el ambiente antes de aplicar

```sql
DESCRIBE FORMATTED <db>.<tabla>;
```

Verificar en la salida:

- `Table Type` → `MANAGED_TABLE` o `EXTERNAL_TABLE`
- `external.table.purge` → **debe ser `true`** en tablas externas, o el mantenimiento no borra nada de S3
- `write.delete.mode` → `merge-on-read` si Impala escribe
- `format-version` → `2`

---

## Disponibilidad de comandos por motor

Validado contra documentación oficial de Cloudera.

| Comando | Impala / Hue | Hive | Spark / CDE |
|---|:--:|:--:|:--:|
| `ALTER TABLE ... DROP PARTITION` | ✔ | ✔ | ✔ |
| `OPTIMIZE TABLE [FILE_SIZE_THRESHOLD_MB=n]` | ✔ | ✔ | vía `rewrite_data_files` |
| `ALTER TABLE ... EXECUTE EXPIRE_SNAPSHOTS(...)` | ✔ | ✔ | vía `CALL system.*` |
| `ALTER TABLE ... EXECUTE REMOVE_ORPHAN_FILES(...)` | ✔ | ✔ | vía `CALL system.*` |
| `COMPUTE STATS ... TABLESAMPLE` | ✔ | ✘ | ✘ |
| `COMPUTE INCREMENTAL STATS` | ✘ **no funcional en Iceberg** | ✘ | ✘ |
| `INVALIDATE METADATA` | ✔ | ✘ | ✘ |
| Escritura en copy-on-write | ✘ **falla** | ✔ | ✔ |
| Borrado directo en S3 | ✘ **NUNCA** | ✘ **NUNCA** | ✘ **NUNCA** |

---

## Contribuciones

La rama `main` está protegida. Los cambios se proponen vía *fork* + *pull request*.
Ver [`CONTRIBUTING.md`](CONTRIBUTING.md) y
[`docs/05-proteccion-repositorio.md`](docs/05-proteccion-repositorio.md).

---

## Aviso

Material técnico de referencia. Validar siempre contra la versión específica de Cloudera
Runtime del ambiente destino antes de aplicar en producción — las capacidades de Iceberg
evolucionan entre releases.
