# Databricks notebook source
# DBTITLE 1,Import Library
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import * 

# COMMAND ----------

# DBTITLE 1,Create Dataframe
df =  spark.read.load('/FileStore/tables/googleplaystore-1.csv',format='csv', sep=',',header='true',escape='"',inferschema='true')

# COMMAND ----------

df.count()

# COMMAND ----------

df.show(1)

# COMMAND ----------

# DBTITLE 1,Check Schema
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Data Cleaning
df= df.drop("Size", "Content Rating","Last Updated", "Android Ver")

# COMMAND ----------

df.show(2)

# COMMAND ----------

df = df.drop('Current Ver')

# COMMAND ----------

df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, col
from pyspark.sql.types import IntegerType

df = df.withColumn("Reviews", col("Reviews").cast(IntegerType())) \
      .withColumn("Installs", regexp_replace(col("Installs"), "[+,]", "")) \
      .withColumn("Installs", regexp_replace(col("Installs"), ",", "")) \
      .withColumn("Installs", col("Installs").cast(IntegerType())) \
      .withColumn("Price", regexp_replace(col("Price"), "[$]", "")) \
      .withColumn("Price", col("Price").cast(IntegerType()))


# COMMAND ----------

df.show(5)

# COMMAND ----------

df.createOrReplaceTempView("apps")

# COMMAND ----------

# MAGIC %sql select * from apps

# COMMAND ----------

# DBTITLE 1,Top Reviews give to apps
# MAGIC %sql select App,sum(Reviews) from apps
# MAGIC group by 1
# MAGIC order by 2 desc

# COMMAND ----------

# DBTITLE 1,Top 10 installs app
# MAGIC %sql select App,sum(Installs) from apps
# MAGIC group by 1
# MAGIC order by 2 desc

# COMMAND ----------

df.createOrReplaceTempView("apps")

# COMMAND ----------

# MAGIC %sql select * from apps

# COMMAND ----------

# DBTITLE 1,Top Reviews give to the apps
# MAGIC %sql select App, sum(Reviews) from apps
# MAGIC group by 1 
# MAGIC order by 2 desc

# COMMAND ----------

# DBTITLE 1,Top 10 Installss Apps
# MAGIC %sql select App,sum(Installs) from apps
# MAGIC group by 1 
# MAGIC order by 2 desc

# COMMAND ----------

# DBTITLE 1,Category wise distribution
# MAGIC %sql select Category,Sum(Installs) from apps
# MAGIC group by 1 
# MAGIC order by 2 desc

# COMMAND ----------

# DBTITLE 1,Top Paid Apps
# MAGIC %sql select App , sum(Price) from apps
# MAGIC where Type = 'Paid'
# MAGIC group by 1 
# MAGIC order by 2 desc
