# Runbook de mantenimiento

## Principio rector

El mantenimiento se agenda en la ventana de **menor concurrencia**, e inmediatamente **antes**
de la ventana de mayor lectura. El objetivo es que las consultas del día encuentren la tabla
ya compactada, sin delete files pendientes.

---

## Calendario para el perfil telco típico

```
00:00 ─ 06:00   CDE escribe CDRs (ingesta masiva, MoR)

06:00 ─ 07:00   ██ VENTANA DE MANTENIMIENTO ██
                ① OPTIMIZE TABLE ... FILE_SIZE_THRESHOLD_MB=256
                ② ALTER TABLE ... EXECUTE EXPIRE_SNAPSHOTS(...)
                ③ ALTER TABLE ... EXECUTE REMOVE_ORPHAN_FILES(...)
                ④ COMPUTE STATS ... TABLESAMPLE SYSTEM(10)   (condicional)
                ⑤ INVALIDATE METADATA                        (si corrió desde CDE)

07:00 ─ 23:59   CDW atiende lecturas — tabla compactada, sin overhead merge-on-read
                └── Reprocesos diurnos: DROP PARTITION + INSERT
                    Si se usa DELETE, ejecutar OPTIMIZE inmediatamente después
```

---

## Frecuencia por tipo de tabla

| Perfil de la tabla | `OPTIMIZE` | `EXPIRE_SNAPSHOTS` |
|---|---|---|
| CDR alta frecuencia (MoR) | Diario | Diario · `NOW() - INTERVAL 7 DAYS` |
| Streaming / CDC (MoR) | Cada 4-6 h | Diario · `NOW() - INTERVAL 1 DAYS` |
| Dimensión batch diario (CoW) | Semanal | Diario · `NOW() - INTERVAL 30 DAYS` |
| Agregados mensuales | Mensual | Mensual · `NOW() - INTERVAL 30 DAYS` |
| Staging efímero | No requerido | Diario · `NOW() - INTERVAL 1 DAYS` |
| Archivo histórico | Tras cada carga | Tras cada carga · `NOW() - INTERVAL 1 DAYS` |

`REMOVE_ORPHAN_FILES` sigue la misma frecuencia que `EXPIRE_SNAPSHOTS`, siempre
inmediatamente después, y con timestamp **posterior** al `OPTIMIZE` de esa misma ejecución.

---

## Disparadores fuera de calendario

| Evento | Acción |
|---|---|
| `DELETE` masivo durante ventana de lectura | `OPTIMIZE` inmediato |
| Evolución de esquema o partition spec | `OPTIMIZE` **completo** (sin umbral) |
| Reproceso de particiones grandes | `EXPIRE_SNAPSHOTS` + `REMOVE_ORPHAN_FILES` |
| Diagnóstico 6.2 muestra delete files acumulados | `OPTIMIZE` |
| Crecimiento inesperado del prefijo S3 | Ejecutar [`04-troubleshooting.md`](04-troubleshooting.md) |

---

## Sobre `FILE_SIZE_THRESHOLD_MB`

| Modo | Comportamiento | Cuándo usarlo |
|---|---|---|
| Con umbral | Selecciona solo archivos pequeños y delete files. Los archivos grandes sin deletes se conservan. | **Mantenimiento recurrente.** Ahorra cómputo significativamente. |
| Sin umbral | Reescribe la tabla completa al esquema y partition spec vigentes. | Tras evolución de esquema o de partition spec. |

> Con umbral especificado, los archivos que no cumplen el criterio **conservan el esquema y
> layout de partición antiguos**. Por eso tras una evolución de spec hay que correr el
> `OPTIMIZE` completo al menos una vez.

Requisitos del comando:

- `write.format.default` debe ser `parquet`
- El usuario necesita privilegio `ALL` sobre la tabla
- No aplica sobre vistas

---

## Sobre el timestamp de `EXPIRE_SNAPSHOTS`

Usar **expresiones relativas** en jobs automatizados evita parametrizar fechas y elimina
una clase entera de errores:

```sql
ALTER TABLE telco_core.cdr_voz EXECUTE EXPIRE_SNAPSHOTS(NOW() - INTERVAL 7 DAYS);
```

La red de seguridad es `history.expire.min-snapshots-to-keep`. **Es un conteo, no un delta
de tiempo.** Documentación Cloudera: si la tabla recibe una modificación por hora, `min=24`
cubre 24 horas; si recibe una por minuto, cubrir 24 horas requiere `min=1440`.

Dimensionamiento para el perfil telco:

| Patrón de escritura | Valor sugerido |
|---|---|
| CDE inserta ~1 vez/minuto durante 6 h (≈360 commits/noche) | `400` |
| Carga diaria única | `30` |
| Solo un punto de rollback | `5` |

---

## Sobre el timestamp de `REMOVE_ORPHAN_FILES`

**Esta es la causa más frecuente de fallo silencioso del mantenimiento.**

`REMOVE_ORPHAN_FILES('<ts>')` solo elimina huérfanos **creados antes** de `<ts>`. Los delete
files que el `OPTIMIZE` acaba de dejar huérfanos tienen timestamp de creación reciente.

Caso real:

```
06:00  OPTIMIZE TABLE ...                              → deja delete files huérfanos
06:30  EXECUTE EXPIRE_SNAPSHOTS('2026-07-27 00:00:00')
06:31  EXECUTE REMOVE_ORPHAN_FILES('2026-07-27 00:00:00')
       → NO elimina nada: los huérfanos se crearon a las 06:00, después de las 00:00
       → Los delete files quedan acumulados en S3 indefinidamente
```

Regla: el timestamp debe ser **posterior al fin del `OPTIMIZE`**. Usar
`NOW() - INTERVAL 1 HOURS` elimina el problema de raíz.

---

## Sobre `INVALIDATE METADATA`

| Origen del mantenimiento | ¿Se requiere? |
|---|---|
| Impala / Hue | **No.** El Catalog Service de CDW propaga los cambios automáticamente. |
| CDE / Spark | **Sí**, ejecutado desde Impala. El HMS event polling sincroniza eventualmente pero no garantiza inmediatez. |

Para automatizarlo sin acceso manual a Hue, ver [`cde/invalidate_metadata.py`](../cde/invalidate_metadata.py):
conecta al Virtual Warehouse de Impala vía `impyla` con LDAP + workload password.

> CDP Public Cloud usa **LDAP**, no Kerberos. Kerberos es el mecanismo de Private Cloud /
> on-premises.
