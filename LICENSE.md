# Aviso de uso

Material técnico de referencia para diseño y operación de tablas Apache Iceberg v2 en
Cloudera Data Warehouse.

## Alcance

Este contenido documenta comportamientos verificados contra la documentación oficial de
Cloudera y Apache Iceberg vigente al momento de su publicación. Las capacidades de Iceberg,
Impala, Hive y CDE evolucionan entre releases de Cloudera Runtime.

**Validar siempre contra la versión específica del ambiente destino antes de aplicar en
producción.**

## Sin garantía

El material se provee "tal cual", sin garantía de ningún tipo. Quien lo aplique es
responsable de validar los comandos en un ambiente de prueba antes de ejecutarlos sobre
datos productivos.

Varios de los comandos documentados eliminan datos de forma irreversible
(`DROP PARTITION`, `EXPIRE_SNAPSHOTS`, `REMOVE_ORPHAN_FILES`). Ejecutarlos con parámetros
incorrectos puede provocar pérdida de información no recuperable.

## Marcas

Apache Iceberg, Apache Impala, Apache Hive y Apache Spark son marcas de la Apache Software
Foundation. Cloudera, CDW, CDE y CDP son marcas de Cloudera, Inc. Este material no está
afiliado ni respaldado oficialmente por dichas organizaciones.
