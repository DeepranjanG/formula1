# Databricks notebook source
storage_account_name = "saformuladl"
storage_account_key = dbutils.secrets.get('scope4formula1', 'storageAccountName')
configs = {f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": f"{storage_account_key}"}

# COMMAND ----------

container_name = "raw"
dbutils.fs.mount(
    source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls("mnt/saformuladl/raw")

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

def mount_adls(container_name):
    dbutils.fs.mount(
    source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)

# COMMAND ----------

mount_adls("processed")

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

spark

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data Ingestion Requirements
# MAGIC 1. Ingest all 8 files into the data lake
# MAGIC 2. Ingested data must have the schema applied
# MAGIC 3. Ingested data must have audit columns
# MAGIC 4. Ingested data must be stored in columnar format (i.e., Parquet)
# MAGIC 5. Must be able to analyze the ingested data via SQL
# MAGIC 6. Ingestion Logic must be able to handle incremental load

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data Transformation requirements
# MAGIC 1. Join the key information required for reporting to create a new table.
# MAGIC 2. Join the key information required for analysis to create a new table.
# MAGIC 3. Transformed tables must have audit columns
# MAGIC 4. Must be able to analyze the transformed data via SQL.
# MAGIC 5. Transformed data must be stored in columnar format (i.e., Parquet)
# MAGIC 6. Transformation logic must be able to handle incremental load

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reporting requirements
# MAGIC 1. Driver Standings
# MAGIC 2. Constructor Standings

# COMMAND ----------

# MAGIC %md
# MAGIC #### Analysis Requirements
# MAGIC 1. Dominant drivers
# MAGIC 2. Dominant teams
# MAGIC 3. Visualize the outputs
# MAGIC 4. Create Databricks Dashboards

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scheduling requirements
# MAGIC 1. Scheduled to run every Sunday 10 PM.
# MAGIC 2. Ability to monitor pipelines.
# MAGIC 3. Ability to re -run failed pipelines.
# MAGIC 4. Ability to set-up alerts on failures.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Other Non-functional requirements
# MAGIC 1. Ability to delete individual records
# MAGIC 2. Ability to see history and time travel
# MAGIC 3. Ability to roll back to a previous version
