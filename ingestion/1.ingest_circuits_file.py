# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest circuits.csv file

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
        .csv('dbfs:/mnt/saformuladl/raw/circuits.csv')

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
circuits_final_df = circuits_renamed_df.withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write data to datalake as parquet format

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet("/mnt/saformuladl/processed/circuits")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/saformuladl/processed/circuits

# COMMAND ----------

display(spark.read.parquet("/mnt/saformuladl/processed/circuits"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data Ingestion - Races

# COMMAND ----------

df_race = spark.read.option("header", True) \
                    .option("printSchema", True) \
                    .csv("/mnt/saformuladl/raw/races.csv")

# COMMAND ----------

display(df_race)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create a new column name race_timestamp using date and time column

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, col, lit, concat
df_renamed_race = df_race.withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(''), col('time')), 'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

display(df_renamed_race)

# COMMAND ----------


