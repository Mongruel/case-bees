# Databricks notebook source
from pyspark.sql.functions import current_date, col, count

# COMMAND ----------

data=[["1"]]
df=spark.createDataFrame(data,["id"])
dt = df.withColumn("current_date",current_date()).collect()[0][1]

# COMMAND ----------

df = spark.read.format("delta").load(f"/mnt/svr/breweries/brewerie{dt}")

# COMMAND ----------

df_grouped = df.groupBy("state", "brewery_type").agg(count("*").alias("breweries_per_state")).orderBy('state')

# COMMAND ----------

df_grouped.write.format("delta").save(f"/mnt/gld/breweries/brewerie_by_state_type{dt}")

# COMMAND ----------

df_grouped.write.mode("overwrite").saveAsTable("brewerie_by_state_type")

# COMMAND ----------

display(df_grouped)
