# Databricks notebook source
# MAGIC %run "/Workspace/Users/zaidnayeem@beable.onmicrosoft.com/trainpred_project/02-silver/02-transformation"

# COMMAND ----------

transit_df.createOrReplaceTempView("silver_trains")

gold_df = spark.sql("""
SELECT
  train_type,
  station_role,
  COUNT(train_number) AS total_active_trains
FROM silver_trains
GROUP BY train_type,station_role
ORDER BY total_active_trains DESC
""")

display(gold_df)

# COMMAND ----------

gold_path = f"abfss://gold@{storage_account_name}.dfs.core.windows.net/train_summary_metrics"

(
    gold_df
    .write
    .format("delta")
    .mode("overwrite")
    .save(gold_path)
)
print("gold layer done")