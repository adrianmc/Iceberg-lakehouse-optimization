# Changelog

Formato basado en [Keep a Changelog](https://keepachangelog.com/es-ES/1.1.0/).
Versionado según [SemVer](https://semver.org/lang/es/).

---

## [1.2.0] — 2026-07-28

### Corregido

- **`cde/maintenance_job.py` usaba sintaxis de Impala dentro de una sesión Spark.**
  El job ejecutaba `OPTIMIZE TABLE` y `ALTER TABLE ... EXECUTE EXPIRE_SNAPSHOTS(...)` a
  través de `spark.sql()`. Spark no entiende esos comandos — son de Impala y Hive. El job
  habría fallado en la primera ejecución. Reescrito con los procedimientos correctos:
  `CALL <catalog>.system.rewrite_data_files`, `expire_snapshots` y `remove_orphan_files`.

  Es exactamente el error que este repositorio documenta como causa raíz: aplicar la
  sintaxis de un motor a otro.

- **Faltaba el paso de `rewrite_position_delete_files` en el camino por Spark.**
  Documentación de Apache Iceberg: tras `rewrite_data_files`, los position delete records
  que apuntan a los data files reescritos no siempre quedan marcados para eliminación y
  pueden seguir referenciados por el snapshot vivo — el problema de *dangling deletes*.
  Omitirlo reproduce el síntoma que motivó este estándar: delete files que sobreviven al
  mantenimiento sin que ningún comando reporte error.

  `OPTIMIZE TABLE` de Impala cubre esto en un solo comando; Spark requiere dos
  procedimientos.

- **`WHERE_FILTER` con comillas simples generaba SQL mal formado.**
  Un filtro como `fecha = '2026-06-15'` embebido en un literal también delimitado por
  comillas simples producía `where => 'fecha = '2026-06-15''`. Se duplican las comillas
  al escapar.

### Añadido

- `docs/07-paridad-caminos.md` — tabla de equivalencias entre motores, asimetrías
  documentadas y procedimiento de failover en ambas direcciones.
- Validación de `RETAIN_LAST >= 1` en el job de Spark: con `0` la tabla queda sin snapshot
  activo.
- Verificación posterior automática: el job consulta `.files` y avisa si quedan delete
  files pendientes.
- Checklist de paridad en `PULL_REQUEST_TEMPLATE.md` para evitar que ambos caminos
  diverjan silenciosamente.
- Variables del camino Spark (`RETAIN_LAST`, `SPARK_CATALOG`, `WHERE_FILTER`) incorporadas
  a `scripts/config.env.example`, de modo que un solo archivo alimente ambos caminos.

### Notas

- **Ambos caminos de ejecución se mantienen deliberadamente.** No son redundantes: cada uno
  cubre un modo de fallo del otro, y sus capacidades no son simétricas.
- El camino por CDE **siempre requiere dos pasos**: `COMPUTE STATS` e `INVALIDATE METADATA`
  son comandos de Impala que Spark no tiene. El camino por impala-shell lo resuelve en uno.
- Solo el camino por Spark permite compactación filtrada por partición y escritura en
  tablas copy-on-write.

---

## [1.1.0] — 2026-07-28

### Añadido

- **Camino de ejecución vía `impala-shell`**, independiente de Spark y CDE:
  - `sql/07-mantenimiento-impala-shell.sql` — secuencia parametrizada con `${var:...}`
  - `scripts/maintenance_impala.sh` — orquestador Bash que itera sobre varias tablas
  - `scripts/config.env.example` — plantilla de configuración con manejo seguro de contraseña
  - `docs/06-ejecucion-impala-shell.md` — instalación, conexión, agendamiento y comparativa

### Notas

- Al ejecutar el mantenimiento desde Impala, **`INVALIDATE METADATA` deja de ser necesario**:
  el Catalog Service de CDW propaga los cambios a los coordinadores automáticamente. El paso
  ⑤ de la secuencia canónica solo aplica cuando el mantenimiento corre desde CDE/Spark.
- Los timestamps pasan a evaluarse **del lado del servidor** (`NOW() - INTERVAL n DAYS`),
  eliminando el desfase de reloj cliente/cluster y el riesgo de zona horaria. Relevante en
  ambientes LATAM: CDW opera en UTC.

### Pendiente de validación en ambiente

- La forma con expresión relativa está confirmada en documentación para `EXPIRE_SNAPSHOTS`
  (`ALTER TABLE ice_t EXECUTE EXPIRE_SNAPSHOTS(now() - interval 10 days)`). Para
  `REMOVE_ORPHAN_FILES` no se encontró confirmación explícita de que acepte expresiones
  además de literales. El script incluye la variante con literal comentada y el orquestador
  ya calcula la variable `ORPHAN_TS` para ese caso.

---

## [1.0.0] — 2026-07-28

### Añadido

- Estándar completo de diseño, retención, borrado y mantenimiento Iceberg v2 para CDW
- Scripts SQL modulares: setup, DDL, borrado, reproceso, mantenimiento, diagnóstico
- Job PySpark de mantenimiento recurrente para CDE
- Script de sincronización de catálogo Impala vía `impyla` (LDAP + workload password)
- Script de inventario S3 (solo lectura) para verificación antes/después
- Documentación: decisión MoR/CoW, catálogo de propiedades, runbook, troubleshooting
- Guía de protección del repositorio

### Correcciones respecto a supuestos iniciales

Estas premisas se asumieron y resultaron incorrectas al verificarlas contra documentación
oficial. Quedan documentadas para evitar que se reintroduzcan:

- **Impala solo escribe en merge-on-read.** Se asumía que copy-on-write era el default y la
  opción recomendada. La documentación de Cloudera es explícita: Impala falla si se configura
  copy-on-write. Solo Hive y Spark soportan ambos modos.
- **`COMPUTE INCREMENTAL STATS` no es funcional en Iceberg.** Impala lo convierte
  internamente en un `COMPUTE STATS` completo.
- **`history.expire.min-snapshots-to-keep` es un conteo, no un delta de tiempo.**
- **`EXPIRE_SNAPSHOTS` no limpia metadata files por default.** Requiere
  `write.metadata.delete-after-commit.enabled=true`.
- **El timestamp de `REMOVE_ORPHAN_FILES` debe ser posterior al `OPTIMIZE`.** Usar la
  medianoche del día de ejecución deja fuera los delete files recién dejados huérfanos, y el
  comando no reporta error.
- **`CALL system.*` es sintaxis de Spark**, no disponible en Impala/Hue. Los equivalentes en
  Impala son `ALTER TABLE ... EXECUTE EXPIRE_SNAPSHOTS(...)` y
  `ALTER TABLE ... EXECUTE REMOVE_ORPHAN_FILES(...)`.
