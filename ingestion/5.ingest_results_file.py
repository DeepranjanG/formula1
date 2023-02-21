# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest results.json file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# COMMAND ----------

results_schema = StructType([StructField("resultId", IntegerType(), False), \
                          StructField("raceId", IntegerType(), False), \
                          StructField("driverId", IntegerType(), False), \
                          StructField("constructId", IntegerType(), False), \
                          StructField("number", IntegerType(), True), \
                          StructField("grid", IntegerType(), False), \
                          StructField("position", IntegerType(), True), \
                          StructField("positionText", StringType(), False), \
                          StructField("positionOrder", IntegerType(), False), \
                          StructField("points", FloatType(), False), \
                          StructField("laps", IntegerType(), False), \
                          StructField("time", StringType(), True), \
                          StructField("milliseconds", IntegerType(), True), \
                          StructField("fastestLap", IntegerType(), True), \
                          StructField("rank", IntegerType(), True), \
                          StructField("fastestLapTime", StringType(), True), \
                          StructField("fastestlapSpeed", StringType(), True), \
                          StructField("statusId", IntegerType(), False)])

# COMMAND ----------

results_df = spark.read \
                    .option("header", True) \
                    .schema(results_schema) \
                    .json("/mnt/saformuladl/raw/results.json")

# COMMAND ----------

display(results_df)

# COMMAND ----------

results_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
results_renamed_df = results_df.withColumnRenamed('resultId', 'result_id') \
                                .withColumnRenamed('raceId', 'race_id') \
                                .withColumnRenamed('driverId', 'driver_id') \
                                .withColumnRenamed('constructId', 'constructor_id') \
                                .withColumnRenamed('positionText', 'position_text') \
                                .withColumnRenamed('positionOrder', 'position_order') \
                                .withColumnRenamed('fastestLap', 'fastest_lap') \
                                .withColumnRenamed('fastestLapTime', 'fastest_lap_time') \
                                .withColumnRenamed('fastestlapSpeed', 'fastest_lap_speed') \
                                .withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

display(results_renamed_df)

# COMMAND ----------

results_final_df = results_renamed_df.drop('statusId')

# COMMAND ----------

results_final_df.write.mode('overwrite').partitionBy('race_id').parquet("/mnt/saformuladl/processed/results")

# COMMAND ----------


