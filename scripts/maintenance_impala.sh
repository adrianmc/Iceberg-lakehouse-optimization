#!/usr/bin/env bash
#
# Mantenimiento de tablas Iceberg v2 vía impala-shell.
#
# Equivalente funcional a cde/maintenance_job.py, sin dependencia de Spark ni de CDE.
# Pensado para ambientes donde la conectividad hacia CDE/Spark es problemática pero
# impala-shell funciona correctamente.
#
# SECUENCIA POR TABLA
#   1. OPTIMIZE TABLE ... FILE_SIZE_THRESHOLD_MB=<umbral>
#   2. ALTER TABLE ... EXECUTE EXPIRE_SNAPSHOTS(NOW() - INTERVAL <n> DAYS)
#   3. ALTER TABLE ... EXECUTE REMOVE_ORPHAN_FILES(NOW() - INTERVAL 1 HOURS)
#   4. COMPUTE STATS ... TABLESAMPLE SYSTEM(10)          (opcional)
#
# NO ejecuta INVALIDATE METADATA: al correr todo desde Impala, el Catalog Service de CDW
# propaga los cambios a los coordinadores automáticamente. Ese paso solo hace falta cuando
# el mantenimiento se ejecuta desde CDE/Spark.
#
# REQUISITOS
#   · impala-shell 2.5 o superior (la sustitución de variables ${var:...} es client-side)
#   · Conectividad al Virtual Warehouse en el puerto 443
#   · Privilegio ALL sobre las tablas (requerido por OPTIMIZE TABLE)
#
# CONFIGURACIÓN
#   Copiar config.env.example a config.env, completar, y NO versionarlo.

set -uo pipefail

# ─────────────────────────────────────────────────────────────────────────────────────────
# Configuración
# ─────────────────────────────────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
SQL_TEMPLATE="${REPO_ROOT}/sql/07-mantenimiento-impala-shell.sql"

CONFIG_FILE="${CONFIG_FILE:-${SCRIPT_DIR}/config.env}"
if [[ -f "${CONFIG_FILE}" ]]; then
    # shellcheck disable=SC1090
    source "${CONFIG_FILE}"
fi

IMPALA_HOST="${IMPALA_HOST:-}"
IMPALA_PORT="${IMPALA_PORT:-443}"
IMPALA_USER="${IMPALA_USER:-}"
IMPALA_PASSWORD_CMD="${IMPALA_PASSWORD_CMD:-}"
TABLAS="${TABLAS:-}"
UMBRAL_MB="${UMBRAL_MB:-256}"
RETENCION_DIAS="${RETENCION_DIAS:-7}"
COLUMNAS_STATS="${COLUMNAS_STATS:-}"
LOG_DIR="${LOG_DIR:-${REPO_ROOT}/logs}"

DRY_RUN=false
SKIP_STATS=false

# ─────────────────────────────────────────────────────────────────────────────────────────
# Argumentos
# ─────────────────────────────────────────────────────────────────────────────────────────
usage() {
    cat <<EOF
Uso: $(basename "$0") [opciones]

Opciones:
  --tablas "db.t1,db.t2"    Lista de tablas (sobrescribe TABLAS de config.env)
  --umbral-mb N             FILE_SIZE_THRESHOLD_MB para OPTIMIZE (default: ${UMBRAL_MB})
  --retencion-dias N        Días de retención de snapshots (default: ${RETENCION_DIAS})
  --columnas "c1, c2"       Columnas para COMPUTE STATS
  --skip-stats              Omitir el paso de COMPUTE STATS
  --dry-run                 Mostrar los comandos sin ejecutarlos
  -h, --help                Esta ayuda

Ejemplo:
  $(basename "$0") --tablas "telco_core.cdr_voz" --umbral-mb 256 --retencion-dias 7
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --tablas)          TABLAS="$2"; shift 2 ;;
        --umbral-mb)       UMBRAL_MB="$2"; shift 2 ;;
        --retencion-dias)  RETENCION_DIAS="$2"; shift 2 ;;
        --columnas)        COLUMNAS_STATS="$2"; shift 2 ;;
        --skip-stats)      SKIP_STATS=true; shift ;;
        --dry-run)         DRY_RUN=true; shift ;;
        -h|--help)         usage; exit 0 ;;
        *) echo "ERROR: opción desconocida: $1" >&2; usage; exit 1 ;;
    esac
done

# ─────────────────────────────────────────────────────────────────────────────────────────
# Validaciones
# ─────────────────────────────────────────────────────────────────────────────────────────
fail() { echo "ERROR: $*" >&2; exit 1; }

command -v impala-shell >/dev/null 2>&1 || fail "impala-shell no está en el PATH."
[[ -n "${IMPALA_HOST}" ]] || fail "IMPALA_HOST no definido. Ver scripts/config.env.example"
[[ -n "${IMPALA_USER}" ]] || fail "IMPALA_USER no definido. Ver scripts/config.env.example"
[[ -n "${TABLAS}"      ]] || fail "No se especificaron tablas. Usar --tablas o config.env"
[[ -f "${SQL_TEMPLATE}" ]] || fail "No se encuentra ${SQL_TEMPLATE}"

if [[ "${SKIP_STATS}" == false && -z "${COLUMNAS_STATS}" ]]; then
    echo "AVISO: COLUMNAS_STATS vacío — se omitirá COMPUTE STATS."
    SKIP_STATS=true
fi

mkdir -p "${LOG_DIR}"

# ─────────────────────────────────────────────────────────────────────────────────────────
# Preparación del SQL
# ─────────────────────────────────────────────────────────────────────────────────────────
# Si se omiten las estadísticas, se genera una variante del template sin esa sección.
SQL_EFECTIVO="${SQL_TEMPLATE}"
TMP_SQL=""
if [[ "${SKIP_STATS}" == true ]]; then
    TMP_SQL="$(mktemp -t iceberg_maint_XXXXXX.sql)"
    # Elimina la línea del COMPUTE STATS y su encabezado de fase
    grep -v -e '^COMPUTE STATS' -e "4/4 · COMPUTE STATS" "${SQL_TEMPLATE}" > "${TMP_SQL}"
    SQL_EFECTIVO="${TMP_SQL}"
    COLUMNAS_STATS="_omitido_"
fi
cleanup() { [[ -n "${TMP_SQL}" && -f "${TMP_SQL}" ]] && rm -f "${TMP_SQL}"; }
trap cleanup EXIT

# Timestamp de respaldo por si el ambiente no acepta expresiones en REMOVE_ORPHAN_FILES.
# El SQL usa NOW() - INTERVAL 1 HOURS; esta variable alimenta la variante comentada.
ORPHAN_TS="$(date -u -d '-1 hour' '+%Y-%m-%d %H:%M:%S' 2>/dev/null \
             || date -u -v-1H '+%Y-%m-%d %H:%M:%S')"

# ─────────────────────────────────────────────────────────────────────────────────────────
# Ejecución
# ─────────────────────────────────────────────────────────────────────────────────────────
IFS=',' read -ra TABLA_LIST <<< "${TABLAS}"
TOTAL=${#TABLA_LIST[@]}
FALLIDAS=()
STAMP="$(date -u '+%Y%m%d_%H%M%S')"

echo "========================================================================================"
echo "MANTENIMIENTO ICEBERG v2 — vía impala-shell"
echo "========================================================================================"
echo "  Host                   : ${IMPALA_HOST}:${IMPALA_PORT}"
echo "  Usuario                : ${IMPALA_USER}"
echo "  Tablas                 : ${TOTAL}"
echo "  Umbral de compactación : ${UMBRAL_MB} MB"
echo "  Retención de snapshots : ${RETENCION_DIAS} días"
echo "  COMPUTE STATS          : $([[ "${SKIP_STATS}" == true ]] && echo 'omitido' || echo "${COLUMNAS_STATS}")"
echo "  Modo                   : $([[ "${DRY_RUN}" == true ]] && echo 'DRY RUN' || echo 'EJECUCIÓN REAL')"
echo "  Logs                   : ${LOG_DIR}"
echo "========================================================================================"

for TABLA in "${TABLA_LIST[@]}"; do
    TABLA="$(echo "${TABLA}" | xargs)"   # trim
    [[ -z "${TABLA}" ]] && continue

    if [[ "${TABLA}" != *.* ]]; then
        echo "  OMITIDA  ${TABLA} — debe tener formato <base>.<tabla>"
        FALLIDAS+=("${TABLA} (formato inválido)")
        continue
    fi

    LOG_FILE="${LOG_DIR}/${TABLA//./_}_${STAMP}.log"

    echo ""
    echo "--- ${TABLA} ---------------------------------------------------------------------"

    IMPALA_ARGS=(
        --protocol=hs2-http
        --ssl
        -i "${IMPALA_HOST}:${IMPALA_PORT}"
        -u "${IMPALA_USER}"
        -l
        --var="TABLA=${TABLA}"
        --var="UMBRAL_MB=${UMBRAL_MB}"
        --var="RETENCION_DIAS=${RETENCION_DIAS}"
        --var="COLUMNAS_STATS=${COLUMNAS_STATS}"
        --var="ORPHAN_TS=${ORPHAN_TS}"
        -f "${SQL_EFECTIVO}"
    )

    # Suministro de la contraseña sin exponerla en la línea de comandos ni en el historial.
    if [[ -n "${IMPALA_PASSWORD_CMD}" ]]; then
        IMPALA_ARGS+=( --ldap_password_cmd="${IMPALA_PASSWORD_CMD}" )
    fi

    if [[ "${DRY_RUN}" == true ]]; then
        echo "  [DRY RUN] impala-shell ${IMPALA_ARGS[*]}"
        continue
    fi

    if impala-shell "${IMPALA_ARGS[@]}" 2>&1 | tee "${LOG_FILE}"; then
        # PIPESTATUS[0] es el código de salida de impala-shell, no el de tee
        if [[ "${PIPESTATUS[0]}" -eq 0 ]]; then
            echo "  OK  ${TABLA}  →  ${LOG_FILE}"
        else
            echo "  FALLO  ${TABLA}  →  ${LOG_FILE}"
            FALLIDAS+=("${TABLA}")
        fi
    else
        echo "  FALLO  ${TABLA}  →  ${LOG_FILE}"
        FALLIDAS+=("${TABLA}")
    fi
done

echo ""
echo "========================================================================================"
if [[ ${#FALLIDAS[@]} -gt 0 ]]; then
    echo "RESULTADO: ${#FALLIDAS[@]} de ${TOTAL} tablas fallaron"
    for t in "${FALLIDAS[@]}"; do echo "  - ${t}"; done
    echo "Revisar los logs en ${LOG_DIR}"
    echo "========================================================================================"
    exit 1
fi

echo "RESULTADO: ${TOTAL} tablas procesadas correctamente"
echo "No se requiere INVALIDATE METADATA: el mantenimiento corrió desde Impala."
echo "========================================================================================"
