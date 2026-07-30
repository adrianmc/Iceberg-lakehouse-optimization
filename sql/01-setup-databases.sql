/* ============================================================================================
   01 · CREACIÓN DE BASES DE DATOS
   Motor: Impala (Hue / Cloudera Data Explorer / impala-shell)
   ============================================================================================

   PRINCIPIO DE SEPARACIÓN DE PREFIJOS S3
   --------------------------------------------------------------------------------------------
   La base de archivo apunta a un BUCKET/PREFIJO DISTINTO del operativo. Esto es
   intencional y no negociable:

   Las S3 Lifecycle Rules (transición a Glacier, expiración de objetos) se aplican
   ÚNICAMENTE sobre el prefijo de archivo. Aplicarlas sobre el prefijo operativo destruiría
   data files que Iceberg aún referencia en snapshots activos, corrompiendo la tabla.
   ============================================================================================ */

-- Capa operativa: tablas de alta frecuencia consultadas por BI y analítica
CREATE DATABASE IF NOT EXISTS telco_core
COMMENT 'Capa operativa — CDR, eventos de red y sesiones. Iceberg v2.'
LOCATION 's3a://<bucket-datalake>/warehouse/telco_core';

-- Staging: landing zone de ingesta y tablas efímeras de reproceso
CREATE DATABASE IF NOT EXISTS telco_staging
COMMENT 'Staging — landing de ingesta y tablas temporales de reproceso.'
LOCATION 's3a://<bucket-datalake>/warehouse/telco_staging';

-- Archivo: histórico frío, prefijo DESACOPLADO apto para S3 Lifecycle Rules
CREATE DATABASE IF NOT EXISTS telco_archive
COMMENT 'Archivo histórico frío. Prefijo desacoplado — apto para S3 Lifecycle Rules.'
LOCATION 's3a://<bucket-archive>/warehouse/telco_archive';


-- Verificación
SHOW DATABASES LIKE 'telco_*';
DESCRIBE DATABASE EXTENDED telco_core;
