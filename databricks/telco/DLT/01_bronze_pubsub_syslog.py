"""
Telco Network Performance — Bronze Syslog Ingestion via Pub/Sub
================================================================
Lakeflow Declarative Pipeline that streams RFC 5424 syslog events from
GCP Pub/Sub topic `syslog-events` into the bronze.syslog_raw streaming table.

Auth: GCP service account credentials stored in Databricks secret scope
`reynolds-pravindev-scope` (sa-client-email, sa-client-id, sa-private-key,
sa-private-key-id).
"""

import dlt
from pyspark.sql.functions import col, current_timestamp


PROJECT_ID = "gcp-sandbox-field-eng"
TOPIC_ID = "syslog-events"
SUBSCRIPTION_ID = "syslog-events-sub"
SECRET_SCOPE = "reynolds-pravindev-scope"


@dlt.table(
    name="syslog_raw",
    comment="Raw RFC 5424 syslog messages streamed from GCP Pub/Sub topic syslog-events",
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true",
        "pipelines.autoOptimize.managed": "true",
    },
)
def syslog_raw():
    return (
        spark.readStream
        .format("pubsub")
        .option("subscriptionId", SUBSCRIPTION_ID)
        .option("topicId", TOPIC_ID)
        .option("projectId", PROJECT_ID)
        .option("clientEmail", dbutils.secrets.get(SECRET_SCOPE, "sa-client-email"))
        .option("clientId", dbutils.secrets.get(SECRET_SCOPE, "sa-client-id"))
        .option("privateKey", dbutils.secrets.get(SECRET_SCOPE, "sa-private-key"))
        .option("privateKeyId", dbutils.secrets.get(SECRET_SCOPE, "sa-private-key-id"))
        .load()
        .select(
            col("messageId").alias("message_id"),
            col("payload").cast("string").alias("value"),
            col("publishTimestampInMillis").alias("publish_ts_millis"),
            current_timestamp().alias("ingestion_timestamp"),
        )
    )
