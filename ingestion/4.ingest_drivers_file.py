# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest drivers.json file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------

name_schema = StructType([StructField("forename", StringType(), True),\
                         StructField("surname", StringType(), True)])

# COMMAND ----------

drivers_schema = StructType([StructField("driverId", IntegerType(), False),
                             StructField("driverRef", StringType(), True),
                             StructField("number", IntegerType(), True),
                             StructField("code", StringType(), True),
                             StructField("name", name_schema),
                             StructField("dob", DateType(), True),
                             StructField("nationality", StringType(), True),
                             StructField("url", StringType(), True)
                            ])

# COMMAND ----------

drivers_df = spark.read \
                .option("header", True) \
                .schema(drivers_schema) \
                .json("/mnt/saformuladl/raw/drivers.json")

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, lit, concat, current_timestamp

# COMMAND ----------

 drivers_with_columns_df = drivers_df.withColumnRenamed("driverId", "driver_id") \
            .withColumnRenamed("driverRef", "driver_ref") \
            .withColumn("ingestion_date", current_timestamp()) \
            .withColumn("name", concat(col('name.forename'), lit(" "), col("name.surname")))

# COMMAND ----------

display(drivers_with_columns_df)

# COMMAND ----------

drivers_final_df = drivers_with_columns_df.drop(col("url"))

# COMMAND ----------

drivers_final_df.write.mode("overwrite").parquet("/mnt/saformuladl/processed/drivers")

# COMMAND ----------

dbutils.notebook.exit("Success")
