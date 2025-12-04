# Databricks notebook source
# MAGIC %md
# MAGIC # Telco Network Performance Demo - SFTP Connection Setup
# MAGIC 
# MAGIC This notebook sets up the Unity Catalog connection to the SFTP server for ingesting network telemetry data.
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC - SFTP server is running and accessible from Databricks workspace
# MAGIC - SFTP credentials are stored in Databricks secrets
# MAGIC - User has CREATE CONNECTION privileges in Unity Catalog
# MAGIC 
# MAGIC ## References
# MAGIC - [Databricks SFTP Connector Documentation](https://docs.databricks.com/gcp/en/ingestion/sftp)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# SFTP Server Configuration
# Update these values with your actual SFTP server details

SFTP_HOST = dbutils.secrets.get(scope="<YOUR_SECRET_SCOPE>", key="SFTP_HOST")
SFTP_USERNAME = dbutils.secrets.get(scope="<YOUR_SECRET_SCOPE>", key="SFTP_USERNAME")
SFTP_PASSWORD = dbutils.secrets.get(scope="<YOUR_SECRET_SCOPE>", key="SFTP_PASSWORD")
SFTP_PORT = "22"

# Connection name in Unity Catalog
CONNECTION_NAME = "telco_network_sftp"

# Catalog and schema for the demo
CATALOG_NAME = "telus_networkperf"
SCHEMA_NAME = "bronze"

print(f"SFTP Host: {SFTP_HOST}")
print(f"SFTP Port: {SFTP_PORT}")
print(f"SFTP Username: {SFTP_USERNAME}")
print(f"Connection Name: {CONNECTION_NAME}")
print(f"Catalog: {CATALOG_NAME}")
print(f"Schema: {SCHEMA_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Databricks Secrets (Optional but Recommended)
# MAGIC 
# MAGIC For production environments, store credentials in Databricks secrets instead of hard-coding them.
# MAGIC 
# MAGIC ```bash
# MAGIC # Using Databricks CLI
# MAGIC databricks secrets create-scope telco-demo
# MAGIC databricks secrets put-secret telco-demo sftp-host --string-value "34.xxx.xxx.xxx"
# MAGIC databricks secrets put-secret telco-demo sftp-username --string-value "telco_user"
# MAGIC databricks secrets put-secret telco-demo sftp-password --string-value "your-password"
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Unity Catalog Objects

# COMMAND ----------

# Create catalog if it doesn't exist
spark.sql(f"""
CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}
COMMENT 'Telco Network Performance Monitoring Data'
""")

print(f"✓ Catalog '{CATALOG_NAME}' created/verified")

# COMMAND ----------

# Create schemas for medallion architecture
schemas = {
    "bronze": "Raw data ingested from SFTP server",
    "silver": "Cleaned and validated network metrics",
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
        
    print(f"Schema '{CATALOG_NAME}.{schema}' created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Unity Catalog SFTP Connection
# MAGIC 
# MAGIC This creates a connection using password-based authentication as specified in the requirements.

# COMMAND ----------

# Drop existing connection if it exists (for re-running the notebook)
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
COMMENT 'SFTP connection for Telco network telemetry data ingestion'
"""

try:
    spark.sql(create_connection_sql)
    print(f"✓ Successfully created connection '{CONNECTION_NAME}'")
except Exception as e:
    print(f"Error creating connection: {e}")
    print("\nNote: If using secrets, update the SQL to use secret() function:")
    print("  user secret('telco-demo', 'sftp-username'),")
    print("  password secret('telco-demo', 'sftp-password')")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Verify Connection

# COMMAND ----------

# Show connection details
connection_info = spark.sql(f"DESCRIBE CONNECTION {CONNECTION_NAME}").toPandas()
display(connection_info)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Test SFTP Access
# MAGIC 
# MAGIC Test reading from the SFTP server to ensure connectivity is working.

# COMMAND ----------

# Test connection by listing files in syslog directory
syslog_path = f"sftp://{SFTP_USERNAME}@{SFTP_HOST}:{SFTP_PORT}/sftp/telco/syslog/"
snmp_path = f"sftp://{SFTP_USERNAME}@{SFTP_HOST}:{SFTP_PORT}/sftp/telco/snmp/"

print("Testing SFTP connection...")
print(f"Syslog path: {syslog_path}")
print(f"SNMP path: {snmp_path}")

# Note: We'll test actual file reading in the ingestion notebook
# For now, just verify the connection was created successfully

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration Summary
# MAGIC 
# MAGIC The following resources have been created:
# MAGIC 
# MAGIC | Resource | Name | Description |
# MAGIC |----------|------|-------------|
# MAGIC | Catalog | `telco_network` | Main catalog for all telco data |
# MAGIC | Schema | `telco_network.bronze` | Raw ingested data from SFTP |
# MAGIC | Schema | `telco_network.silver` | Cleaned and validated data |
# MAGIC | Schema | `telco_network.gold` | Aggregated metrics and dimensions |
# MAGIC | Schema | `telco_network.metrics` | Unity Catalog metric views |
# MAGIC | Connection | `telco_network_sftp` | SFTP connection to telco server |
# MAGIC 
# MAGIC ## Next Steps
# MAGIC 
# MAGIC 1. Run `01_bronze_ingestion.py` to start ingesting data from SFTP
# MAGIC 2. Set up the silver layer transformations
# MAGIC 3. Create gold layer aggregations
# MAGIC 4. Define metric views for monitoring

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (Optional)
# MAGIC 
# MAGIC Run this cell only if you need to completely reset the demo environment.

# COMMAND ----------

# Uncomment to reset everything
# spark.sql(f"DROP CONNECTION IF EXISTS {CONNECTION_NAME}")
# spark.sql(f"DROP CATALOG IF EXISTS {CATALOG_NAME} CASCADE")
# print("✓ Cleanup complete")

