#!/usr/bin/env bash
#
# Inventario de un prefijo S3 de tabla Iceberg — SOLO LECTURA.
#
# Uso previsto: medir el estado antes y despues de una ventana de mantenimiento para
# verificar que el espacio se libero efectivamente.
#
#   ./s3_inventory.sh s3://mi-bucket/warehouse/telco_core/cdr_voz  antes
#   # ... ejecutar mantenimiento ...
#   ./s3_inventory.sh s3://mi-bucket/warehouse/telco_core/cdr_voz  despues
#   diff inventario_antes.txt inventario_despues.txt
#
# ADVERTENCIA
# -----------
# Este script NO borra nada y nunca debe modificarse para hacerlo. Eliminar objetos
# directamente de S3 sobre tablas Iceberg rompe el catalogo: los snapshots quedan apuntando
# a archivos inexistentes y el dano puede ser irrecuperable.
# La unica forma valida de liberar espacio es EXPIRE_SNAPSHOTS + REMOVE_ORPHAN_FILES.

set -euo pipefail

PREFIX="${1:-}"
LABEL="${2:-$(date +%Y%m%d_%H%M%S)}"

if [[ -z "$PREFIX" ]]; then
    echo "Uso: $0 <s3://bucket/prefijo/tabla> [etiqueta]" >&2
    exit 1
fi

if [[ "$PREFIX" != s3://* ]]; then
    echo "ERROR: el prefijo debe comenzar con s3://" >&2
    exit 1
fi

OUT="inventario_${LABEL}.txt"

echo "Inventariando ${PREFIX} ..."

aws s3 ls "${PREFIX}/" --recursive > "/tmp/_inv_raw.txt"

TOTAL_FILES=$(wc -l < /tmp/_inv_raw.txt)
TOTAL_BYTES=$(awk '{s+=$3} END {print s+0}' /tmp/_inv_raw.txt)
DATA_FILES=$(grep -c '/data/' /tmp/_inv_raw.txt || true)
META_FILES=$(grep -c '/metadata/' /tmp/_inv_raw.txt || true)
DELETE_FILES=$(grep -c 'delete-' /tmp/_inv_raw.txt || true)
DELETE_BYTES=$(grep 'delete-' /tmp/_inv_raw.txt | awk '{s+=$3} END {print s+0}' || echo 0)

{
    echo "==============================================================================="
    echo "INVENTARIO S3 — ${LABEL}"
    echo "==============================================================================="
    echo "Prefijo            : ${PREFIX}"
    echo "Fecha              : $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
    echo "-------------------------------------------------------------------------------"
    printf "Objetos totales    : %'d\n"  "${TOTAL_FILES}"
    printf "Tamano total       : %.2f GB\n" "$(echo "${TOTAL_BYTES}/1024/1024/1024" | bc -l)"
    echo "-------------------------------------------------------------------------------"
    printf "Data files         : %'d\n" "${DATA_FILES}"
    printf "Metadata files     : %'d\n" "${META_FILES}"
    printf "Delete files       : %'d  (%.2f MB)\n" \
        "${DELETE_FILES}" "$(echo "${DELETE_BYTES}/1024/1024" | bc -l)"
    echo "==============================================================================="
    echo ""
    echo "Nota: si 'Delete files' > 0 despues del mantenimiento, revisar:"
    echo "  1. Se ejecuto OPTIMIZE TABLE antes de EXPIRE_SNAPSHOTS?"
    echo "  2. El timestamp de REMOVE_ORPHAN_FILES es posterior al OPTIMIZE?"
    echo "  3. La tabla es EXTERNAL con external.table.purge='true'?"
    echo "  Ver docs/04-troubleshooting.md"
} | tee "${OUT}"

rm -f /tmp/_inv_raw.txt
echo ""
echo "Guardado en ${OUT}"
