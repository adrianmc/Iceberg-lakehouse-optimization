# Ejecución vía impala-shell (alternativa a CDE)

Camino de ejecución independiente de Spark y de CDE. Útil cuando la conectividad hacia
CDE es problemática pero impala-shell funciona correctamente.

---

## Qué cambia respecto al camino por CDE

| Aspecto | CDE / Spark | impala-shell |
|---|---|---|
| **Iteración sobre tablas** | Bucle `for` en Python | Bucle en Bash (`maintenance_impala.sh`) |
| **Cálculo de timestamps** | `datetime` en Python, lado cliente | `NOW() - INTERVAL n DAYS`, **lado servidor** |
| **`INVALIDATE METADATA`** | **Requerido** — paso adicional vía impyla | **No requerido** — el Catalog Service propaga solo |
| **Parametrización** | Variables de entorno del job | `--var=NOMBRE=valor` → `${var:NOMBRE}` |
| **Manejo de errores** | `try/except` por tabla | Código de salida de `impala-shell` por tabla |
| **Agendamiento** | Scheduler nativo de CDE | `cron`, Airflow, Control-M, Autosys |
| **Credenciales** | Variables de entorno inyectadas por CDE | `--ldap_password_cmd` hacia un gestor de secretos |
| **Dónde corre** | Dentro del cluster CDE | Cualquier host con acceso al VW por 443 |
| **Dependencias** | PySpark + impyla | Solo impala-shell |

### Dos ventajas reales del camino por impala-shell

**1 · Desaparece el paso de `INVALIDATE METADATA`.**
Cuando el mantenimiento corre desde Impala, el Catalog Service de CDW propaga los cambios
a todos los coordinadores automáticamente. El paso ⑤ de la secuencia canónica deja de
existir, junto con toda la complejidad de conexión `impyla` + LDAP que exigía.

**2 · Los timestamps se evalúan del lado del servidor.**
`NOW() - INTERVAL 7 DAYS` lo resuelve el cluster. Se elimina el desfase de reloj entre el
host que lanza el job y el cluster, y el riesgo de zona horaria — que en este contexto no
es teórico: los timestamps de CDW son UTC y los ambientes LATAM operan en UTC-5.

---

## Instalación de impala-shell

Si el host ya forma parte de un cluster CDP, impala-shell ya está instalado.

Para un host externo (jump host, VM de automatización, contenedor):

```bash
pip install impala-shell
impala-shell --version
```

> La sustitución de variables `${var:...}` es una función **del cliente**, no del backend.
> Requiere impala-shell 2.5 o superior.

---

## Conexión a CDW Public Cloud

Obtener la cadena exacta desde la interfaz:

```
CDW → Virtual Warehouses → tile del VW de Impala
    → menú de opciones (⋮) → Copy Impala shell command
```

El comando copiado tiene esta forma:

```bash
impala-shell --protocol='hs2-http' --ssl -i "coordinator-vw-impala.dw-xxxxx.cloudera.site:443"
```

Para autenticación LDAP con usuario de workload se agrega `-u` y `-l`:

```bash
impala-shell \
    --protocol='hs2-http' \
    --ssl \
    -i "coordinator-vw-impala.dw-xxxxx.cloudera.site:443" \
    -u "svc_mantenimiento_iceberg" \
    -l
```

| Bandera | Función |
|---|---|
| `--protocol='hs2-http'` | Transporte HTTP sobre HiveServer2 |
| `--ssl` | TLS — obligatorio, protege las credenciales LDAP en tránsito |
| `-i host:443` | Coordinador del Virtual Warehouse |
| `-u <usuario>` | Usuario de workload de CDP |
| `-l` | Habilita autenticación LDAP |

> CDP Public Cloud usa **LDAP con workload password**, no Kerberos. Kerberos es el
> mecanismo de Private Cloud / on-premises.

A diferencia de `impyla`, impala-shell **no requiere `http_path`** por separado: la ruta va
implícita en el endpoint del VW.

---

## Contraseña sin exponerla

Con `-l` y sin contraseña, impala-shell la solicita de forma interactiva. Eso sirve para
ejecución manual pero no para `cron`.

Para automatización se usa `--ldap_password_cmd`, que recibe un **comando** cuya salida por
stdout es la contraseña. Así nunca aparece en la línea de comandos, en `ps`, ni en el
historial del shell:

```bash
# Gestor de secretos (recomendado en producción)
--ldap_password_cmd="aws secretsmanager get-secret-value \
    --secret-id iceberg/impala --query SecretString --output text"

--ldap_password_cmd="vault kv get -field=password secret/cdp/impala"

# Archivo local con permisos restrictivos
--ldap_password_cmd="cat /etc/cdp/.impala_pw"    # chmod 600
```

> Verificar la disponibilidad de la bandera en la versión instalada con
> `impala-shell --help | grep -i password`. Las opciones varían entre versiones.

---

## Uso

### Configuración inicial

```bash
cp scripts/config.env.example scripts/config.env
chmod 600 scripts/config.env
$EDITOR scripts/config.env
```

### Prueba en seco

```bash
./scripts/maintenance_impala.sh --dry-run
```

Imprime los comandos que se ejecutarían, sin tocar nada.

### Una tabla

```bash
./scripts/maintenance_impala.sh \
    --tablas "telco_core.cdr_voz" \
    --umbral-mb 256 \
    --retencion-dias 7 \
    --columnas "fecha_evento, msisdn_origen, id_celda_origen"
```

### Varias tablas, sin estadísticas

```bash
./scripts/maintenance_impala.sh \
    --tablas "telco_core.cdr_voz,telco_core.cdr_datos,telco_core.eventos_red_kpi" \
    --skip-stats
```

### Una sola sentencia suelta

```bash
impala-shell --protocol='hs2-http' --ssl \
    -i "<host>:443" -u "<usuario>" -l \
    -q 'OPTIMIZE TABLE telco_core.cdr_voz FILE_SIZE_THRESHOLD_MB=256'
```

> Las comillas de `-q` deben ser **simples**. Con comillas dobles, Bash intentaría expandir
> `${var:...}` antes de que impala-shell lo reciba.

---

## Agendamiento con cron

```bash
crontab -e
```

```cron
# Mantenimiento Iceberg — 06:00 diario, después de la ingesta de CDE
# Zona horaria del host: verificar con `timedatectl` que coincida con la operación
0 6 * * * /opt/iceberg-standard/scripts/maintenance_impala.sh \
            >> /var/log/iceberg_maint.log 2>&1
```

Consideraciones:

- El script devuelve **código de salida 1** si alguna tabla falla — encadenable con
  alertamiento.
- Cada tabla genera su propio log en `LOG_DIR`, con nombre `<db>_<tabla>_<timestamp>.log`.
- Verificar la zona horaria del host que ejecuta el cron. Los timestamps de CDW son UTC.

### Integración con schedulers empresariales

En entornos telco suele existir Control-M, Autosys o Airflow. El script está diseñado para
integrarse: devuelve códigos de salida convencionales (`0` éxito, `1` fallo) y escribe todo
a stdout/stderr, sin requerir terminal interactiva.

```bash
# Airflow — BashOperator
BashOperator(
    task_id="iceberg_maintenance",
    bash_command="/opt/iceberg-standard/scripts/maintenance_impala.sh "
                 "--tablas 'telco_core.cdr_voz' --umbral-mb 256",
)
```

---

## Sustitución de variables

Sintaxis confirmada en la documentación de Impala:

| Contexto | Sintaxis |
|---|---|
| Línea de comandos | `--var=nombre=valor` |
| Dentro de un script `-f` | `SET VAR:nombre=valor;` |
| Referencia en SQL | `${var:nombre}` |

Ejemplo de la documentación oficial:

```bash
impala-shell --var=tname=table1 --var=colname=x --var=coltype=string \
    -q 'CREATE TABLE ${var:tname} (${var:colname} ${var:coltype}) STORED AS PARQUET'
```

> Si un comentario dentro del SQL contiene `${...}` y **no** es una variable, hay que
> escapar el `$`: `-- \${ejemplo}`. De lo contrario impala-shell intentará sustituirlo.

---

## Punto a validar en el ambiente

La forma con expresión relativa está **confirmada en la documentación de Cloudera para
`EXPIRE_SNAPSHOTS`**:

```sql
ALTER TABLE ice_t EXECUTE EXPIRE_SNAPSHOTS(now() - interval 10 days);
```

Para `REMOVE_ORPHAN_FILES` **no encontramos confirmación explícita** de que acepte
expresiones además de literales de timestamp. El script usa la forma con expresión porque
es la que elimina la clase de error del timestamp desfasado, pero:

**Si `REMOVE_ORPHAN_FILES(NOW() - INTERVAL 1 HOURS)` falla con error de parseo**, usar la
variante con literal que está comentada en `sql/07-mantenimiento-impala-shell.sql`. El
script Bash ya calcula y pasa la variable `ORPHAN_TS` para ese caso — solo hay que
descomentar la línea y comentar la otra.

Vale la pena validar esto una vez en el ambiente y dejar registrado el resultado.

---

## Cuándo seguir usando CDE

El camino por impala-shell no reemplaza a CDE en todos los casos:

| Situación | Camino recomendado |
|---|---|
| Conectividad a CDE problemática | **impala-shell** |
| Mantenimiento simple sobre tablas Impala | **impala-shell** — más directo, sin `INVALIDATE METADATA` |
| Compactación filtrada por partición | **CDE/Spark** — Impala no soporta filtro en `OPTIMIZE TABLE` |
| Tablas copy-on-write | **CDE/Spark o Hive** — Impala no escribe CoW |
| Mantenimiento como parte de un pipeline Spark mayor | **CDE** |
| Orquestación con dependencias complejas entre jobs | **CDE** o scheduler empresarial |
