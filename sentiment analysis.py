# Databricks notebook source
import pyspark
import pandas as pd
import numpy as np
# import nltk
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession 
from pyspark.sql import functions as func
from pyspark.sql.types import StringType,FloatType

# COMMAND ----------

spark = SparkSession.builder.master("local[*]").appName("TwitterSentiment").getOrCreate()

# COMMAND ----------

spark

# COMMAND ----------

#creating spark dataframe
df = spark.read.csv("dbfs:/mnt/saformuladl/raw/data.csv")
df.show(truncate=False)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df = df.withColumnRenamed('_c0','target').withColumnRenamed('_c1','id').withColumnRenamed('_c2','date')\
.withColumnRenamed('_c3','flag').withColumnRenamed('_c4','user').withColumnRenamed('_c5','text')

# COMMAND ----------

df.show(5)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.select([func.count(func.when(func.isnan(c),c)).alias(c) for c in df.columns]).toPandas().head()

# COMMAND ----------

df = df.dropDuplicates()

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC - We only need the target and the text column for sentiment analyses, that's why dropping the rest of the columns.

# COMMAND ----------

df = df.drop("id","date","flag","user")

# COMMAND ----------

df

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.select("target").distinct().show()

# COMMAND ----------

df.createOrReplaceTempView('temp')
df = spark.sql('SELECT CASE target WHEN 4 THEN 1.0  ELSE 0 END AS label, text FROM temp')
df.show(5, truncate = False)

# COMMAND ----------

df.tail(5)

# COMMAND ----------

df.groupby("label").count().show()

# COMMAND ----------

!pip install nltk

# COMMAND ----------

import nltk
from nltk.corpus import stopwords
from  nltk.stem import SnowballStemmer
import re

# COMMAND ----------

nltk.download('stopwords')
stop_words = stopwords.words("english")
stemmer = SnowballStemmer("english")
text_cleaning_re = "@\S+|https?:\S+|http?:\S|[^A-Za-z0-9]+"

# COMMAND ----------

def preprocess(text, stem=False):
    # Remove link,user and special characters
    text = re.sub(text_cleaning_re, ' ', str(text).lower()).strip()
    tokens = []
    for token in text.split():
        if token not in stop_words:
            if stem:
                tokens.append(stemmer.stem(token))
            else:
                tokens.append(token)
    return " ".join(tokens)

# COMMAND ----------

# MAGIC %%time
# MAGIC clean_text = func.udf(lambda x: preprocess(x), StringType())
# MAGIC df = df.withColumn('text_cleaned',clean_text(func.col("text")))

# COMMAND ----------

df.show(5, truncate=False)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC - ## Preparing Data for Model Building

# COMMAND ----------

from pyspark.ml.feature import Tokenizer

# COMMAND ----------

tokenizer = Tokenizer(inputCol="text_cleaned", outputCol="words_tokens")
words_tokens = tokenizer.transform(df)
words_tokens.show()

# COMMAND ----------


