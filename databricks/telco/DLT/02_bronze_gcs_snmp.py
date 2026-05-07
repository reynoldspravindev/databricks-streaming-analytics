"""
Telco Network Performance — Bronze SNMP Ingestion via GCS Auto Loader
======================================================================
Streams JSON Lines (NDJSON) SNMP metric files from
gs://reynolds-telco-bucket/snmp/ into the bronze.snmp_raw streaming table
using Auto Loader file-event mode (managed Pub/Sub on the UC external location).
"""

import dlt
from pyspark.sql.functions import col, current_timestamp


SNMP_GCS_PATH = "gs://reynolds-telco-bucket/snmp/"
SCHEMA_LOCATION = "/Volumes/telco_networkperf/bronze/schemas/snmp"


@dlt.table(
    name="snmp_raw",
    comment="Raw SNMP metrics ingested from GCS via Auto Loader (file-event mode)",
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true",
        "pipelines.autoOptimize.managed": "true",
    },
)
def snmp_raw():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("cloudFiles.useManagedFileEvents", "true")
        .option("pathGlobFilter", "*.json")
        .option("rescuedDataColumn", "_rescued_data")
        .load(SNMP_GCS_PATH)
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
    )
