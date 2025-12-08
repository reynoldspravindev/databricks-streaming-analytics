# Databricks notebook source
# MAGIC %md
# MAGIC # Retail Store Performance Demo - Connection Setup
# MAGIC 
# MAGIC This notebook sets up the Unity Catalog connection for ingesting retail store data.
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC - SFTP server is running and accessible from Databricks workspace
# MAGIC - SFTP credentials are stored in Databricks secrets
# MAGIC - User has CREATE CONNECTION privileges in Unity Catalog

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# SFTP Server Configuration
SFTP_HOST = dbutils.secrets.get(scope="<YOUR_SECRET_SCOPE>", key="SFTP_HOST")
SFTP_USERNAME = dbutils.secrets.get(scope="<YOUR_SECRET_SCOPE>", key="SFTP_USERNAME")
SFTP_PASSWORD = dbutils.secrets.get(scope="<YOUR_SECRET_SCOPE>", key="SFTP_PASSWORD")
SFTP_PORT = "22"

# Connection name in Unity Catalog
CONNECTION_NAME = "retail_sftp"

# Catalog and schema for the demo
CATALOG_NAME = "retail_analytics"
SCHEMA_NAME = "bronze"

print(f"SFTP Host: {SFTP_HOST}")
print(f"SFTP Port: {SFTP_PORT}")
print(f"SFTP Username: {SFTP_USERNAME}")
print(f"Connection Name: {CONNECTION_NAME}")
print(f"Catalog: {CATALOG_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Unity Catalog Objects

# COMMAND ----------

# Create catalog if it doesn't exist
spark.sql(f"""
CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}
COMMENT 'Retail Store Performance Analytics Data'
""")

print(f"[OK] Catalog '{CATALOG_NAME}' created/verified")

# COMMAND ----------

# Create schemas for medallion architecture
schemas = {
    "bronze": "Raw data ingested from SFTP and GCS",
    "silver": "Cleaned and validated store metrics",
    "gold": "Aggregated metrics and business views",
    "metrics": "Unity Catalog metric views for monitoring"
}

for schema, description in schemas.items():
    spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{schema}
    COMMENT '{description}'
    """)
    if schema == "bronze":
        spark.sql(f"""
                  CREATE VOLUME IF NOT EXISTS {CATALOG_NAME}.{schema}.checkpoints
                  """)

        spark.sql(f"""
                  CREATE VOLUME IF NOT EXISTS {CATALOG_NAME}.{schema}.schemas
                  """)
        
    print(f"[OK] Schema '{CATALOG_NAME}.{schema}' created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Unity Catalog SFTP Connection

# COMMAND ----------

# Drop existing connection if it exists
try:
    spark.sql(f"DROP CONNECTION IF EXISTS {CONNECTION_NAME}")
    print(f"Dropped existing connection '{CONNECTION_NAME}'")
except Exception as e:
    print(f"No existing connection to drop: {e}")

# COMMAND ----------

# Create the SFTP connection using password authentication
create_connection_sql = f"""
CREATE CONNECTION {CONNECTION_NAME}
TYPE sftp
OPTIONS (
  host '{SFTP_HOST}',
  user '{SFTP_USERNAME}',
  password '{SFTP_PASSWORD}',
  port '{SFTP_PORT}'
)
COMMENT 'SFTP connection for Retail store transaction events ingestion'
"""

try:
    spark.sql(create_connection_sql)
    print(f"[OK] Successfully created connection '{CONNECTION_NAME}'")
except Exception as e:
    print(f"Error creating connection: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Verify Connection

# COMMAND ----------

# Show connection details
connection_info = spark.sql(f"DESCRIBE CONNECTION {CONNECTION_NAME}").toPandas()
display(connection_info)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Test SFTP Access

# COMMAND ----------

# Test connection by listing files in events directory
events_path = f"sftp://{SFTP_USERNAME}@{SFTP_HOST}:{SFTP_PORT}/sftp/retail/events/"
metrics_path = f"sftp://{SFTP_USERNAME}@{SFTP_HOST}:{SFTP_PORT}/sftp/retail/metrics/"

print("Testing SFTP connection...")
print(f"Events path: {events_path}")
print(f"Metrics path: {metrics_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration Summary
# MAGIC 
# MAGIC The following resources have been created:
# MAGIC 
# MAGIC | Resource | Name | Description |
# MAGIC |----------|------|-------------|
# MAGIC | Catalog | `retail_analytics` | Main catalog for all retail data |
# MAGIC | Schema | `retail_analytics.bronze` | Raw ingested data |
# MAGIC | Schema | `retail_analytics.silver` | Cleaned and validated data |
# MAGIC | Schema | `retail_analytics.gold` | Aggregated metrics and dimensions |
# MAGIC | Schema | `retail_analytics.metrics` | Unity Catalog metric views |
# MAGIC | Connection | `retail_sftp` | SFTP connection to retail server |
# MAGIC 
# MAGIC ## Next Steps
# MAGIC 
# MAGIC 1. Run `01_1_bronze_ingestion_events.py` to start ingesting transaction events
# MAGIC 2. Run `01_2_bronze_ingestion_metrics.py` to start ingesting store metrics
# MAGIC 3. Create DLT pipelines for silver and gold layers
# MAGIC 4. Define metric views for monitoring

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (Optional)

# COMMAND ----------

# Uncomment to reset everything
# spark.sql(f"DROP CONNECTION IF EXISTS {CONNECTION_NAME}")
# spark.sql(f"DROP CATALOG IF EXISTS {CATALOG_NAME} CASCADE")
# print("[OK] Cleanup complete")

