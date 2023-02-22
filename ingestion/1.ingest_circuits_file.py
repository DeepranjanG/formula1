# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest circuits.csv file

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /mnt/saformuladl/raw

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# COMMAND ----------

circuits_schema = StructType([
    StructField("circuitId", IntegerType(), False),
    StructField("circuitRef", StringType(), True),
    StructField("name", StringType(), False),
    StructField("location", StringType(), True),
    StructField("country", StringType(), False),
    StructField("lat", DoubleType(), True),
    StructField("lng", DoubleType(), True),
    StructField("alt", IntegerType(), True),
    StructField("url", StringType())
])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the CSV file using the spark dataframe reader

# COMMAND ----------

circuits_df = spark.read \
        .option("header", True) \
        .schema(circuits_schema) \
        .csv(f'{raw_folder_path}/circuits.csv')

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

circuits_df.describe().show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select only required columns

# COMMAND ----------

circuits_df.columns

# COMMAND ----------

circuits_selected_df = circuits_df.select('circuitId',
 'circuitRef',
 'name',
 'location',
 'country',
 'lat',
 'lng',
 'alt')

# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename the columns as required

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
                                            .withColumnRenamed("circuitRef", "circuit_ref") \
                                            .withColumnRenamed("lat", "latitude") \
                                            .withColumnRenamed("lng", "longitude") \
                                            .withColumnRenamed("alt", "altitude")

# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write data to datalake as parquet format

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

display(spark.read.parquet("/mnt/saformuladl/processed/circuits"))

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------


