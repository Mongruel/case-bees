# Databricks notebook source
from pyspark.sql.functions import col, current_date, when, lit, concat

# COMMAND ----------

dbutils.fs.mount(
source = "wasbs://brz@stbees.blob.core.windows.net",
mount_point = "/mnt/brz",
extra_configs = {"fs.azure.account.key.stbees.blob.core.windows.net":dbutils.secrets.get(scope = "keystbees", key = "keystbees")})

# COMMAND ----------

data=[["1"]]
df=spark.createDataFrame(data,["id"])
dt = df.withColumn("current_date",current_date()).collect()[0][1]

# COMMAND ----------

df = spark.read.json(f"/mnt/brz/breweries/breweries{dt}")

# COMMAND ----------

df_transformed = df.withColumn(
    'address_1'
    , when(
        col('address_2').isNotNull()
        , concat(col('address_1'), lit(', '), col('address_2')))
    .otherwise(col('address_1')))

# COMMAND ----------

df_renamed = df_transformed.withColumnRenamed('address_1', 'address').drop('address_2', 'address_3')

# COMMAND ----------

df_renamed = df_renamed.withColumns({
    'address': when(col('address').isNull(), lit('Not informed')).otherwise(col('address'))
    , 'brewery_type': when(col('brewery_type').isNull(), lit('Not informed')).otherwise(col('brewery_type'))
    , 'street': when(col('street').isNull(), lit('Not informed')).otherwise(col('street'))
    , 'website_url': when(col('website_url').isNull(), lit('Not informed')).otherwise(col('website_url'))
    , 'phone': when(col('phone').isNull(), lit('Not informed')).otherwise(col('phone'))
    })

# COMMAND ----------

df_renamed.write.partitionBy('state').format("delta").save(f"/mnt/svr/breweries/brewerie{dt}")

# COMMAND ----------

dbutils.fs.unmount("/mnt/brz")
