# Databricks notebook source
# MAGIC %run "/Workspace/Users/zaidnayeem@beable.onmicrosoft.com/trainpred_project/00-common/common"

# COMMAND ----------

landing_path = f"abfss://landing@{storage_account_name}.dfs.core.windows.net/"
bronze_path = f"abfss://bronze@{storage_account_name}.dfs.core.windows.net/train_data"
checkpoint_path = f"abfss://bronze@{storage_account_name}.dfs.core.windows.net/checkpoint/train_data"
schema_path = f"abfss://bronze@{storage_account_name}.dfs.core.windows.net/schemas/train_data"

print("starting auto loader ")

bronze_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudfiles.schemaLocation", schema_path)
    .load(landing_path)
    )

query = (
    bronze_stream
    .writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow = True)
    .start(bronze_path)
)

query.awaitTermination()

print("success")

# COMMAND ----------

display(spark.read.format("delta").load(bronze_path))