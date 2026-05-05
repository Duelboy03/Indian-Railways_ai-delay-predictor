# Databricks notebook source
# MAGIC %run "/Workspace/Users/zaidnayeem@beable.onmicrosoft.com/trainpred_project/01-bronze/01-ingestion"

# COMMAND ----------

from pyspark.sql.functions import col, explode, when

bronze_df = spark.read.format("delta").load(bronze_path)

exploded_df = bronze_df.withColumn("train", explode(col("data")))

silver_df = exploded_df.select(
    col("timestamp"),
    col("train.trainNumber").alias("train_number"),
    col("train.trainName").alias("train_name"),
    col("train.trainType").alias("train_type"),
    col("train.arrivalTime").alias("arrival_time"),
    col("train.departureTime").alias("departure_time"),
)

display(silver_df)

# COMMAND ----------

#arrival times are way off. lets fix it now 

from pyspark.sql.functions import col, when

cleaned_silver_df = (
    silver_df
    .withColumn(
        "arrival_time",
        when(col("arrival_time") == "00:00", None)
        .when(~col("arrival_time").rlike("^([0-1]?[0-9]|2[0-3]):[0-5][0-9]$"), None)
        .otherwise(col("arrival_time"))
    )
    .withColumn(
        "departure_time",
        when(col("departure_time") == "00:00", None)
        .when(~col("departure_time").rlike("^([0-1]?[0-9]|2[0-3]):[0-5][0-9]$"), None)
        .otherwise(col("departure_time"))
    )
)

display(cleaned_silver_df)

# COMMAND ----------

# for trains in transit 

transit_df = (
    cleaned_silver_df
    .withColumn(
        "station_role",
        when(col("arrival_time").isNull(), "Originating")
        .when(col("departure_time").isNull(), "Terminating")
        .otherwise("Passing Through")
    )
)

display(transit_df)

# COMMAND ----------

silver_path = f"abfss://silver@{storage_account_name}.dfs.core.windows.net/train_data_cleaned"

(
    transit_df
    .write
    .format("delta")
    .mode("append")
    .save(silver_path)
)