# Databricks notebook source
# MAGIC %md
# MAGIC ### Read CSV file using the spark dataframe readr API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------

races_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("year", IntegerType(), True),
    StructField("round", IntegerType(), True),
    StructField("circuitId", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("date", DateType(), True),
    StructField("time", StringType(), True),
    StructField("url", StringType(), True) 
])

# COMMAND ----------

df_race = spark.read.option("header", True) \
                    .schema(races_schema) \
                    .csv("/mnt/saformuladl/raw/races.csv")

# COMMAND ----------

display(df_race)

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, col, lit, concat, current_timestamp
races_with_timestamp_df = df_race.withColumn("ingestion_date", current_timestamp()) \
                                    .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

display(races_with_timestamp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Select only the columns required

# COMMAND ----------

races_with_timestamp_df.columns

# COMMAND ----------

races_selected_df = races_with_timestamp_df.select(col('raceId').alias('race_id'),
                              col('year').alias('race_year'),
                              col('round'),
                              col('circuitId').alias('circuit_id'),
                              col('name'),
                              col('ingestion_date'),
                              col('race_timestamp'))

# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

races_selected_df.write.mode('overwrite').partitionBy('race_year').parquet('/mnt/saformuladl/processed/races')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/saformuladl/processed/races

# COMMAND ----------


