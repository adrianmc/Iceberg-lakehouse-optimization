#!/usr/bin/env python3
"""
Job de mantenimiento recurrente para tablas Iceberg v2 en Cloudera CDE (Spark).

Camino alterno: scripts/maintenance_impala.sh ejecuta lo equivalente via impala-shell.
Ambos se mantienen deliberadamente. Ver docs/07-paridad-caminos.md.

SECUENCIA (procedimientos de Spark, NO sintaxis de Impala)
-----------------------------------------------------------------------------------------
    1. rewrite_data_files            -> compacta y aplica delete files
    2. rewrite_position_delete_files -> elimina "dangling deletes" que deja el paso 1
    3. expire_snapshots              -> elimina snapshots viejos y sus data files
    4. remove_orphan_files           -> elimina archivos sin referencia en ningun snapshot

DIFERENCIAS FRENTE AL CAMINO POR IMPALA
-----------------------------------------------------------------------------------------
Spark NO entiende `OPTIMIZE TABLE` ni `ALTER TABLE ... EXECUTE EXPIRE_SNAPSHOTS(...)`.
Esa es sintaxis de Impala y de Hive. En Spark se usan procedimientos `CALL system.*`.

Ademas, `OPTIMIZE TABLE` de Impala hace en un solo comando lo que en Spark requiere dos
procedimientos. Documentacion de Apache Iceberg sobre rewrite_position_delete_files:

    "After rewrite_data_files, position delete records pointing to the rewritten data
     files are not always marked for removal, and can remain tracked by the table's live
     snapshot metadata. This is known as the 'dangling delete' problem."

Omitir el paso 2 en el camino por Spark reproduce el mismo sintoma que motivo este
repositorio: delete files que sobreviven al mantenimiento.

LIMITACION DEL CAMINO POR SPARK
-----------------------------------------------------------------------------------------
Este job NO ejecuta COMPUTE STATS ni INVALIDATE METADATA: ambos son comandos de Impala.
Tras ejecutarlo hay que correr invalidate_metadata.py contra el Virtual Warehouse de
Impala. Por eso el camino por CDE siempre requiere dos pasos, mientras que el camino por
impala-shell lo resuelve en uno.

CONFIGURACION
-----------------------------------------------------------------------------------------
Variables de entorno del job CDE. Los nombres coinciden con scripts/config.env del camino
por Impala, para que ambos caminos compartan la misma configuracion sin divergir.

    TABLAS                  Lista separada por comas. Ej: "telco_core.cdr_voz,telco_core.cdr_datos"
    RETENCION_DIAS          Dias de retencion de snapshots. Default: 7
    RETAIN_LAST             Snapshots minimos a conservar. Default: 2. Nunca 0.
    UMBRAL_MB               Tamano objetivo de archivo en MB. Default: 256
    ORPHAN_MARGIN_MINUTES   Margen para remove_orphan_files. Default: 60
    SPARK_CATALOG           Catalogo Iceberg. Default: spark_catalog
    WHERE_FILTER            Filtro opcional para rewrite_data_files. Ver advertencia abajo.
    DRY_RUN                 "true" para solo imprimir los comandos
"""

import os
import sys
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession


def env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def parse_tables() -> list:
    raw = env("TABLAS") or env("ICEBERG_TABLES")
    if not raw:
        sys.exit("ERROR: la variable TABLAS es obligatoria.")
    tables = [t.strip() for t in raw.split(",") if t.strip()]
    for t in tables:
        if "." not in t:
            sys.exit(f"ERROR: '{t}' debe tener formato <base>.<tabla>.")
    return tables


def run(spark, label: str, sql: str, dry_run: bool) -> None:
    print(f"  [{label}]")
    print(f"      {sql}")
    if not dry_run:
        rows = spark.sql(sql).collect()
        if rows:
            print(f"      -> {rows[0]}")


def main() -> int:
    tables = parse_tables()
    retention_days = int(env("RETENCION_DIAS", "7"))
    retain_last = int(env("RETAIN_LAST", "2"))
    threshold_mb = int(env("UMBRAL_MB", "256"))
    orphan_margin = int(env("ORPHAN_MARGIN_MINUTES", "60"))
    catalog = env("SPARK_CATALOG", "spark_catalog")
    where_filter = env("WHERE_FILTER")
    dry_run = env("DRY_RUN", "false").lower() == "true"

    if retain_last < 1:
        sys.exit("ERROR: RETAIN_LAST debe ser >= 1. Con 0 la tabla queda sin snapshot activo.")

    target_bytes = threshold_mb * 1024 * 1024
    expire_ts = (datetime.now(timezone.utc) - timedelta(days=retention_days)) \
        .strftime("%Y-%m-%d %H:%M:%S")

    spark = SparkSession.builder.appName("iceberg-maintenance").getOrCreate()

    print("=" * 88)
    print("MANTENIMIENTO ICEBERG v2 - via CDE / Spark")
    print("=" * 88)
    print(f"  Catalogo               : {catalog}")
    print(f"  Tablas                 : {len(tables)}")
    print(f"  Retencion de snapshots : {retention_days} dias (expirar anteriores a {expire_ts} UTC)")
    print(f"  retain_last            : {retain_last}")
    print(f"  Tamano objetivo        : {threshold_mb} MB")
    print(f"  Filtro WHERE           : {where_filter or '(ninguno - tabla completa)'}")
    print(f"  Modo                   : {'DRY RUN' if dry_run else 'EJECUCION REAL'}")
    print("=" * 88)

    failures = []

    for table in tables:
        print(f"\n--- {table} " + "-" * max(0, 84 - len(table)))
        try:
            # 1 - Compactacion. Equivalente Spark de OPTIMIZE TABLE.
            opts = f"map('target-file-size-bytes','{target_bytes}')"
            if where_filter:
                # ADVERTENCIA: el parametro `where` existe en la especificacion Iceberg,
                # pero hay reportes de NoSuchElementException cuando el filtro involucra
                # partition transforms (days(), months()). Validar antes de usarlo en
                # produccion; si falla, omitir WHERE_FILTER y compactar la tabla completa.
                #
                # El filtro casi siempre contiene comillas simples (fecha = '2026-06-15').
                # Al embeberlo dentro de un literal tambien delimitado por comillas simples
                # hay que duplicarlas, o el SQL queda mal formado y falla el parseo.
                escaped_filter = where_filter.replace("'", "''")
                sql = (
                    f"CALL {catalog}.system.rewrite_data_files("
                    f"table => '{table}', "
                    f"strategy => 'binpack', "
                    f"where => '{escaped_filter}', "
                    f"options => {opts})"
                )
            else:
                sql = (
                    f"CALL {catalog}.system.rewrite_data_files("
                    f"table => '{table}', "
                    f"strategy => 'binpack', "
                    f"options => {opts})"
                )
            run(spark, "1/4 rewrite_data_files", sql, dry_run)

            # 2 - Dangling deletes. NO tiene equivalente separado en Impala: OPTIMIZE
            # TABLE ya lo cubre. En Spark es obligatorio, o quedan position deletes
            # referenciados por el snapshot vivo que ningun expire/orphan eliminara.
            sql = (
                f"CALL {catalog}.system.rewrite_position_delete_files("
                f"table => '{table}')"
            )
            run(spark, "2/4 rewrite_position_delete_files", sql, dry_run)

            # 3 - Expiracion de snapshots.
            sql = (
                f"CALL {catalog}.system.expire_snapshots("
                f"table => '{table}', "
                f"older_than => TIMESTAMP '{expire_ts}', "
                f"retain_last => {retain_last})"
            )
            run(spark, "3/4 expire_snapshots", sql, dry_run)

            # 4 - Huerfanos. El timestamp se calcula AQUI, despues de los pasos
            # anteriores, para que cubra los archivos que la compactacion acaba de dejar
            # huerfanos. Usar la medianoche del dia los dejaria fuera de rango y
            # sobrevivirian en S3 sin que el comando reporte error alguno.
            orphan_ts = (datetime.now(timezone.utc) - timedelta(minutes=orphan_margin)) \
                .strftime("%Y-%m-%d %H:%M:%S")
            sql = (
                f"CALL {catalog}.system.remove_orphan_files("
                f"table => '{table}', "
                f"older_than => TIMESTAMP '{orphan_ts}')"
            )
            run(spark, "4/4 remove_orphan_files", sql, dry_run)

            # Verificacion de resultado
            if not dry_run:
                pending = spark.sql(
                    f"SELECT COUNT(*) AS n FROM {table}.files WHERE content IN (1, 2)"
                ).collect()[0]["n"]
                if pending:
                    print(f"      AVISO: quedan {pending} delete files. "
                          f"Ver docs/04-troubleshooting.md")
                else:
                    print("      Sin delete files pendientes.")

            print(f"  OK  {table}")

        except Exception as exc:  # noqa: BLE001
            print(f"  FALLO  {table}: {exc}")
            failures.append((table, str(exc)))

    print("\n" + "=" * 88)
    if failures:
        print(f"RESULTADO: {len(failures)} de {len(tables)} tablas fallaron")
        for table, err in failures:
            print(f"  - {table}: {err}")
        print("")
        print("Si el fallo es de conectividad hacia CDE, el camino alterno es:")
        print('  ./scripts/maintenance_impala.sh --tablas "<lista>"')
        print("=" * 88)
        spark.stop()
        return 1

    print(f"RESULTADO: {len(tables)} tablas procesadas correctamente")
    print("")
    print("PASO PENDIENTE - este job corrio en Spark, no en Impala:")
    print("  1. INVALIDATE METADATA -> ejecutar cde/invalidate_metadata.py")
    print("  2. COMPUTE STATS       -> comando de Impala, no disponible en Spark")
    print("     Ver docs/07-paridad-caminos.md")
    print("=" * 88)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
