# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest qualifying_table folder

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

qualifying_table_schema = StructType([
    StructField("qualifyId", IntegerType(), False),
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), False),
    StructField("constructorId", IntegerType(), False),
    StructField("number", IntegerType(), False),
    StructField("position", IntegerType(), True),
    StructField("q1", StringType(),True),
    StructField("q2", StringType(),True),
    StructField("q3", StringType(),True)
])

# COMMAND ----------

qualifying_table_df = spark.read \
                    .schema(qualifying_table_schema) \
                    .option("multiline", True) \
                    .json("/mnt/saformuladl/raw/qualifying")

# COMMAND ----------

display(qualifying_table_df)

# COMMAND ----------

qualifying_table_df.count()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
qualifying_table_final_df = qualifying_table_df.withColumnRenamed("raceId", "race_id") \
                                    .withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("qualifyId", "qualify_id") \
                                    .withColumnRenamed("constructorId", "constructor_id") \
                                    .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

qualifying_table_final_df.write.mode("overwrite").parquet("/mnt/saformuladl/processed/qualifying")

# COMMAND ----------

dbutils.notebook.exit("Success")
