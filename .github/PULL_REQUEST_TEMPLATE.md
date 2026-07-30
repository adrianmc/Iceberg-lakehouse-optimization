## Descripción

<!-- Qué cambia y por qué -->

## Tipo de cambio

- [ ] `fix` — corrección de comando, sintaxis o error factual
- [ ] `feat` — nuevo patrón, script o sección
- [ ] `docs` — solo documentación
- [ ] `chore` — mantenimiento del repo, CI, formato

---

## Evidencia

> Este repositorio existe porque se aplicaron patrones de Hive a Iceberg sin verificar.
> Todo cambio que afirme un comportamiento de la plataforma requiere evidencia.

- [ ] Enlace a documentación oficial de Cloudera o Apache Iceberg
- [ ] Salida real de una ejecución en un ambiente CDW
- [ ] Referencia a JIRA de Impala / Hive / Iceberg

**Fuente:**

<!-- Pegar enlace, salida de consola o número de JIRA -->

**Ambiente donde se validó:**

- Cloudera Runtime: <!-- ej. 7.3.1 -->
- Motor: <!-- Impala / Hive / Spark-CDE -->
- Tipo de despliegue: <!-- Public Cloud / Private Cloud Base -->

---

## Checklist para cambios en `sql/`

- [ ] El comando fue ejecutado en un ambiente CDW real
- [ ] Se indica el motor donde corre
- [ ] Si la operación modifica datos, la secuencia incluye `EXPIRE_SNAPSHOTS`
- [ ] Si la tabla es MoR, la secuencia incluye `OPTIMIZE` **antes** de expirar
- [ ] El timestamp de `REMOVE_ORPHAN_FILES` es posterior al `OPTIMIZE`

## Checklist de paridad entre caminos de ejecución

> El repo mantiene dos implementaciones (`impala-shell` y `CDE/Spark`) deliberadamente.
> El riesgo real es que diverjan en silencio.

- [ ] ¿El cambio aplica también al otro camino de ejecución?
- [ ] Si aplica, ¿se implementó con la **sintaxis correcta de ese motor**?
      (`OPTIMIZE TABLE` es de Impala/Hive · `CALL system.*` es de Spark — no son intercambiables)
- [ ] ¿Los nombres de variable de configuración siguen coincidiendo entre ambos?
- [ ] ¿Se actualizó la tabla de equivalencias de `docs/07-paridad-caminos.md`?
- [ ] ¿Se probó `--dry-run` en ambos caminos?

## Checklist de seguridad

- [ ] Sin nombres de clientes, buckets, esquemas o tablas productivas reales
- [ ] Sin credenciales, tokens ni contraseñas
- [ ] Los placeholders siguen la convención `<placeholder>`
- [ ] Ninguna instrucción implica borrar archivos directamente de S3
