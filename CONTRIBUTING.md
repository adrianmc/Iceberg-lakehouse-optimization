# Guía de contribución

La rama `main` está protegida. **Nadie hace push directo**, incluido el mantenedor: todo
cambio entra por pull request.

---

## Flujo

### 1 · Fork y rama de trabajo

```bash
gh repo fork <owner>/<repo> --clone
cd <repo>
git checkout -b fix/descripcion-corta
```

Convención de nombres de rama:

| Prefijo | Uso |
|---|---|
| `fix/` | Corrección de un comando, sintaxis o error factual |
| `feat/` | Nuevo patrón, script o sección |
| `docs/` | Solo documentación |
| `chore/` | Mantenimiento del repo, CI, formato |

### 2 · Validar localmente

```bash
# Los archivos SQL no deben contener credenciales ni nombres reales de clientes
grep -rniE "(password|secret|api[_-]?key)\s*=" sql/ cde/ scripts/

# Verificar que no quedaron nombres de objetos de clientes reales.
# Ajustar la lista de patrones a los nombres del cliente con el que se trabajó.
PATRONES_CLIENTE="nombre_base_cliente|prefijo_tabla_cliente"
grep -rniE "$PATRONES_CLIENTE" sql/ docs/
```

### 3 · Commit

Formato [Conventional Commits](https://www.conventionalcommits.org/):

```
fix(sql): corrige timestamp de REMOVE_ORPHAN_FILES en secuencia de mantenimiento

REMOVE_ORPHAN_FILES solo elimina huérfanos creados ANTES del timestamp indicado.
Usar la medianoche del día de ejecución deja fuera los delete files que el OPTIMIZE
acaba de dejar huérfanos. Se cambia a expresión relativa NOW() - INTERVAL 1 HOURS.

Ref: docs/04-troubleshooting.md paso 3
```

### 4 · Pull request

```bash
git push origin fix/descripcion-corta
gh pr create --fill
```

---

## Criterio de aceptación

Todo cambio que afirme un comportamiento de la plataforma **debe venir acompañado de
evidencia**. Este repositorio existe precisamente porque los patrones heredados de Hive se
aplicaron a Iceberg sin verificar.

Se acepta como evidencia:

- Enlace a documentación oficial de Cloudera o Apache Iceberg
- Salida real de una ejecución en un ambiente CDW, con el comando y su resultado
- Referencia a un JIRA de Impala / Hive / Iceberg

**No se acepta:**

- "En Spark funciona así" como justificación para Impala
- Sintaxis tomada de documentación de Athena, Databricks, Dremio o Snowflake sin validar en CDW
- Comandos no ejecutados en un ambiente real

### Checklist específico para cambios en `sql/`

- [ ] El comando fue ejecutado en un ambiente CDW real
- [ ] Se indica el motor donde corre (Impala / Hive / Spark-CDE)
- [ ] Si la operación modifica datos, la secuencia incluye `EXPIRE_SNAPSHOTS`
- [ ] Si la tabla es MoR, la secuencia incluye `OPTIMIZE` **antes** de expirar
- [ ] No hay nombres de clientes, buckets ni credenciales reales
- [ ] Los placeholders siguen la convención `<placeholder>`

---

## Qué no se acepta bajo ninguna circunstancia

- Cualquier instrucción que implique borrar archivos directamente de S3 sobre tablas Iceberg
- Comandos con credenciales embebidas
- Nombres reales de clientes, buckets, esquemas o tablas productivas
