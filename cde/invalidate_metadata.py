#!/usr/bin/env python3
"""
Sincronizacion del catalogo de Impala tras un mantenimiento ejecutado desde CDE/Spark.

Por que existe este script
--------------------------
INVALIDATE METADATA es un comando de Impala. No tiene sentido ejecutarlo desde Spark: Spark
no habla con el Catalog Service de Impala. Cuando el mantenimiento corre en CDE, los cambios
quedan en el HMS y en los metadata files de Iceberg en S3, pero el catalogo de Impala puede
tardar en reflejarlos.

El HMS event polling de CDW sincroniza eventualmente, pero no garantiza inmediatez. Para un
job nocturno seguido de lecturas al amanecer, el comando explicito es mas seguro.

Autenticacion
-------------
CDP Public Cloud usa LDAP con workload password sobre HTTPS puerto 443.
NO usa Kerberos (Kerberos es el mecanismo de Private Cloud / on-premises).

El host y http_path se obtienen desde:
    CDW -> Virtual Warehouse -> menu de opciones -> Copy JDBC String

Ejemplo de JDBC String:
    jdbc:impala://<host>.cloudera.site:443/;ssl=1;transportMode=http;
    httpPath=<path>/cdp-proxy-api/impala;AuthMech=3;

Variables de entorno del job CDE (nunca hardcodear credenciales):
    IMPALA_VW_HOST          Host del Virtual Warehouse
    IMPALA_HTTP_PATH        httpPath del JDBC string
    CDP_WORKLOAD_USER       Usuario CDP
    CDP_WORKLOAD_PASSWORD   Workload password (CDP > Manage Access > Workload Password)
    ICEBERG_TABLES          Lista separada por comas
"""

import os
import sys

from impala.dbapi import connect


REQUIRED = [
    "IMPALA_VW_HOST",
    "IMPALA_HTTP_PATH",
    "CDP_WORKLOAD_USER",
    "CDP_WORKLOAD_PASSWORD",
    "ICEBERG_TABLES",
]


def main() -> int:
    missing = [v for v in REQUIRED if not os.environ.get(v)]
    if missing:
        sys.exit(f"ERROR: faltan variables de entorno: {', '.join(missing)}")

    tables = [t.strip() for t in os.environ["ICEBERG_TABLES"].split(",") if t.strip()]

    print("=" * 88)
    print("SINCRONIZACION DE CATALOGO IMPALA")
    print("=" * 88)
    print(f"  Host   : {os.environ['IMPALA_VW_HOST']}")
    print(f"  Usuario: {os.environ['CDP_WORKLOAD_USER']}")
    print(f"  Tablas : {len(tables)}")
    print("=" * 88)

    conn = connect(
        host               = os.environ["IMPALA_VW_HOST"],
        port               = 443,
        auth_mechanism     = "LDAP",
        use_ssl            = True,
        use_http_transport = True,
        http_path          = os.environ["IMPALA_HTTP_PATH"],
        user               = os.environ["CDP_WORKLOAD_USER"],
        password           = os.environ["CDP_WORKLOAD_PASSWORD"],
    )

    failures = []
    try:
        cursor = conn.cursor()
        for table in tables:
            try:
                print(f"  INVALIDATE METADATA {table}")
                cursor.execute(f"INVALIDATE METADATA {table}")
                print(f"  OK  {table}")
            except Exception as exc:  # noqa: BLE001
                print(f"  FALLO  {table}: {exc}")
                failures.append((table, str(exc)))
        cursor.close()
    finally:
        conn.close()

    print("=" * 88)
    if failures:
        print(f"RESULTADO: {len(failures)} de {len(tables)} tablas fallaron")
        return 1

    print(f"RESULTADO: {len(tables)} tablas sincronizadas")
    return 0


if __name__ == "__main__":
    sys.exit(main())
