# Decisión: Merge-on-Read vs Copy-on-Write

## Primera pregunta — elimina la mayoría de los casos

**¿Qué motor ejecuta los `DELETE` / `UPDATE` / `MERGE` sobre esta tabla?**

| Motor que escribe | Modos disponibles |
|---|---|
| Solo Impala | **MoR obligatorio** |
| Hive | MoR o CoW |
| Spark / CDE | MoR o CoW |
| Mixto (Impala + Spark) | **MoR obligatorio** — es el denominador común |

> Documentación Cloudera — *Row-level operations*:
> *"Impala supports only the MOR mode and will fail if configured for copy-on-write.
> Impala does support reading copy-on-write tables."*

Una tabla en copy-on-write que reciba un `DELETE` desde Impala **falla la operación**. No es
una degradación silenciosa ni un fallback: es un error.

### Regla de oro

Si existe cualquier posibilidad de que un operador ejecute un `DELETE` manual desde Hue o
impala-shell sobre la tabla, la tabla **debe** ser MoR.

---

## Segunda pregunta — solo si el motor permite elegir

| Característica del workload | Modo | Razón |
|---|---|---|
| Ingesta streaming / CDC | MoR | Escrituras eficientes, sin write amplification |
| Escritura frecuente (horaria) | MoR | El costo de reescribir archivos no compensa |
| Porcentaje de datos que cambia: bajo | MoR | Pocos delete files acumulados |
| Lecturas muy frecuentes | CoW | Sin read amplification |
| Updates / deletes masivos | CoW | Evita generar miles de delete files |
| Batch diario de actualización | CoW | Una reescritura vs N delete files |
| Porcentaje de datos que cambia: alto | CoW | La compactación deja de compensar |

---

## Aplicación al perfil telco

| Tabla | Modo | Justificación |
|---|---|---|
| `cdr_voz` / `cdr_datos` / `cdr_sms` | MoR | Ingesta nocturna masiva desde CDE, lecturas diurnas intensas. Requiere `OPTIMIZE` nocturno obligatorio antes de la ventana de lectura. |
| `eventos_red_kpi` | MoR | Append-only. Casi sin `DELETE` → los delete files no se acumulan. |
| `dim_suscriptor` (SCD tipo 2) | CoW | Updates masivos diarios, consultada en joins constantemente. **Los `UPDATE` deben correr en Spark/CDE o Hive, nunca desde Impala.** |
| `agregados_facturacion` | Mixto | `delete`/`update` = MoR (borrados puntuales rápidos), `merge` = CoW (upsert masivo sin generar delete files). Solo escribible desde Spark/CDE. |

---

## Consecuencia operativa de elegir MoR

MoR traslada el costo de la escritura a la lectura. Cada delete file acumulado es una capa
adicional que el motor debe aplicar en cada consulta.

**La compactación no es opcional en MoR — es parte del contrato.** Sin `OPTIMIZE` periódico:

- El rendimiento de lectura se degrada linealmente con la cantidad de delete files
- `EXPIRE_SNAPSHOTS` no libera los data files base, porque los delete files siguen
  referenciándolos
- El costo de almacenamiento crece sin que el borrado lógico se traduzca en ahorro real

Ver [`03-runbook-mantenimiento.md`](03-runbook-mantenimiento.md) para el calendario.

---

## Cómo verificar el modo actual de una tabla

```sql
DESCRIBE FORMATTED telco_core.cdr_voz;
```

Buscar en la salida:

```
write.delete.mode    merge-on-read | copy-on-write
write.update.mode    merge-on-read | copy-on-write
write.merge.mode     merge-on-read | copy-on-write
```

Si las propiedades no aparecen, aplica el default del motor.

## Cómo cambiar el modo

```sql
ALTER TABLE telco_core.cdr_voz SET TBLPROPERTIES (
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode'  = 'merge-on-read'
);
```

Es un cambio de metadata: instantáneo, independiente del tamaño de la tabla. No reescribe
datos existentes — los archivos ya escritos conservan su formato.
