# Databricks notebook source
# MAGIC %md #### Ingest constructors.json file

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read \
                        .schema(constructors_schema) \
                        .json('/mnt/saformuladl/raw/constructors.json')

# COMMAND ----------

constructor_df.printSchema()

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

constructor_dropped_df = constructor_df.drop('url')

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed('constructorId', 'constructor_id') \
                        .withColumnRenamed('constructorRef', 'constructor_ref') \
                        .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

constructor_final_df.write.mode('overwrite').parquet('/mnt/saformuladl/processed/construcors')

# COMMAND ----------

spark

# COMMAND ----------


