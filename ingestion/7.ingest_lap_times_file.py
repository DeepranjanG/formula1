# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest lap_times folder

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

lap_times_schema = StructType([
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), False),
    StructField("lap", IntegerType(), False),
    StructField("position", IntegerType(), True),
    StructField("time", StringType(), False),    
    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

lap_times_df = spark.read \
                    .schema(lap_times_schema) \
                    .csv("/mnt/saformuladl/raw/lap_times")

# COMMAND ----------

display(lap_times_df)

# COMMAND ----------

lap_times_df.count()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
lap_times_renamed_df = lap_times_df.withColumnRenamed("raceId", "race_id") \
                                    .withColumnRenamed("driverId", "driver_id") \
                                    .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

lap_times_renamed_df.write.mode("overwrite").parquet("/mnt/saformuladl/processed/lap_times")

# COMMAND ----------

dbutils.notebook.exit("Success")
