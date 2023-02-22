# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest pit_stops.json file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

pit_schema = StructType([
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), False),
    StructField("stop", IntegerType(), False),
    StructField("lap", IntegerType(), False),
    StructField("time", StringType(), False),
    StructField("duration", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

pit_stops_df = spark.read \
                    .schema(pit_schema) \
                    .option("multiline", True) \
                    .json("/mnt/saformuladl/raw/pit_stops.json")

# COMMAND ----------

display(pit_stops_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
pit_stops_renamed_df = pit_stops_df.withColumnRenamed("raceId", "race_id") \
                                    .withColumnRenamed("driverId", "driver_id") \
                                    .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

pit_stops_renamed_df.write.mode("overwrite").parquet("/mnt/saformuladl/processed/pit_stops")

# COMMAND ----------

dbutils.notebook.exit("Success")
