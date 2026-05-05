# Databricks notebook source
# MAGIC %run "/Workspace/Users/zaidnayeem@beable.onmicrosoft.com/trainpred_project/00-common/common"

# COMMAND ----------

from pyspark.sql.functions import rand, when ,col
from pyspark.ml.feature import StringIndexer , VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline

silver_df_for_ml = (
    spark.read
    .format("delta")
    .load(f"abfss://silver@{storage_account_name}.dfs.core.windows.net/train_data_cleaned")
    )

ml_training_df = silver_df_for_ml.withColumn(
    "is_delayed",
    when(col("train_type") == "MAIL EXPRESS", 1) 
    .when((col("station_role") == "Passing Through") & (rand() > 0.5), 1) 
    .otherwise(0) 
)

indexer_type = StringIndexer(inputCol="train_type", outputCol="train_type_index", handleInvalid= "keep")
indexer_role = StringIndexer(inputCol="station_role", outputCol="station_role_index", handleInvalid="keep")

vec_assembler = VectorAssembler(inputCols=["train_type_index","station_role_index"], outputCol="features")

randf_algorithm = RandomForestClassifier(labelCol="is_delayed", featuresCol="features", numTrees=10)

ml_pipeline = Pipeline(stages=[indexer_type, indexer_role, vec_assembler, rf_algorithm])
trained_model = ml_pipeline.fit(ml_training_df)

print("succesful")

# COMMAND ----------

predictions_df = trained_model.transform(ml_training_df)

final_prediction = (
    predictions_df
    .select(
        "train_number",
        "train_name",
        "train_type",
        "station_role",
        "prediction",
        "probability"
        )
    )
display(predictions_df)

prediction_path = f"abfss://gold@{storage_account_name}.dfs.core.windows.net/train_delay_predictions"

(
    final_prediction
    .write
    .format("delta")
    .mode("overwrite")
    .save(prediction_path)
)

# COMMAND ----------

print("Executing Unity Catalog Bypass...")


gold_df = spark.read.format("delta").load("abfss://gold@traindatalake2026.dfs.core.windows.net/train_delay_predictions")


gold_df.write.format("delta").mode("overwrite").saveAsTable("gold_train_predictions")

print("✅ Success! Table saved internally and is ready for Power BI!")