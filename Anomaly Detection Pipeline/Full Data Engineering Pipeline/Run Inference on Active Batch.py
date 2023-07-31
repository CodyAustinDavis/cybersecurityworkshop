# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Build rolling real-time features and run anomaly detection inference on a streaming dataset!

# COMMAND ----------

# MAGIC %pip install databricks-kakapo

# COMMAND ----------

# DBTITLE 1,Load Model
  from mlflow.tracking.client import MlflowClient
  import mlflow
  import kakapo
  from pyspark.sql.functions import *
  import pandas as pd

  model_name = "uba_amomaly_model"

  client = MlflowClient()
  model_stage = ["Production"]
  model_version = client.get_latest_versions(model_name, stages=model_stage)[0].version
  model_uri = "models:/{model_name}/{model_stage}".format(model_name=model_name, model_stage=model_stage[0])
  prod_model = mlflow.pyfunc.load_model(model_uri)

  print(f"Loading model {model_name} - {model_stage[0]} to generate anomaly predictions...")

# COMMAND ----------

spark.sql("USE cyberworkshop")

# COMMAND ----------

# DBTITLE 1,Define Active Batch - Can be hourly batch, or daily batch, inside or outside a stream
## for example purposes on synthetic data, we will just define the batch manually as the most recent month (excluded during training)
batch_min_ts = "2022-09-22T00:00:00"

## Days of pattern history to predict on
lookback_period_window_days = 30

## Real-world ingestion options
# 1: Spark.readStream("prod_silver_events")
# 2: spark.table.filter(col("event_ts") >= lit(min_batch_ts)) where ts is generated dynamically from manual batch selection or incremental logic
active_silver_events = spark.table("""prod_silver_events""").filter(col("event_ts") >= batch_min_ts)

most_recent_ts_in_batch = active_silver_events.select("event_ts").agg(max("event_ts").alias("max_ts")).collect()[0][0]

print(f"Generating Features on active batch from {batch_min_ts} rolling up to {most_recent_ts_in_batch}")

# COMMAND ----------

# DBTITLE 1,Take Feature Engineering Logic and Apply To Active Batch with Rolling Window
## Wrap all into active batch inference / rolling feature generation function
def generate_rolling_feature_df(input_batch_df) -> DataFrame:

  input_batch_df.createOrReplaceTempView("active_silver_batch")

  df_user_features_rolling_in_batch = spark.sql(f"""
  -- For rolling metrics, we still need to join to silver table to get the longest history we require for feature lookback
  -- i.e. If my batch is hourly, but I need 7 day user history to generate patterns, join to silver table to get 7 day pattern on user

  WITH batch_users AS 
  (
    SELECT DISTINCT CONCAT('user_', entity_id) AS entity_id 
    FROM active_silver_batch
  ), 

  e AS (
      SELECT rolling_events.*
            FROM prod_silver_edges AS rolling_events
            INNER JOIN batch_users as active_users ON active_users.entity_id = rolling_events.src
            WHERE event_ts >= ('{most_recent_ts_in_batch}'::TIMESTAMP - INTERVAL {lookback_period_window_days} DAYS) -- Define rolling window size here
    ),

  rolling_login_stats_by_user AS (
    SELECT
    entity_id,
    CONCAT('user_', entity_id) AS user_join_key,
    MIN(event_ts) AS min_ts,
    '{most_recent_ts_in_batch}'::TIMESTAMP AS max_ts,
    datediff('{most_recent_ts_in_batch}'::TIMESTAMP, MIN(event_ts)) + 1 AS LogDurationInDays,
    COUNT(CASE WHEN event_type = 'login_failed' THEN id ELSE NULL END) AS TotalFailedAttempts,
    COUNT(CASE WHEN event_type = 'login_attempt' THEN id ELSE NULL END) AS TotalLoginAttempts
    FROM (SELECT rolling_events.*
          FROM prod_silver_events AS rolling_events --silver events, NOT the graph data model
          INNER JOIN batch_users as active_users ON active_users.entity_id = rolling_events.entity_id
          WHERE event_ts >= ('{most_recent_ts_in_batch}'::TIMESTAMP - INTERVAL {lookback_period_window_days} DAYS) -- Define rolling window size here
    ) AS curr
    WHERE entity_type = 'name' 
    AND event_type IN ('login_failed', 'login_succeeded', 'login_attempt')
    GROUP BY entity_id

  ),

  rolling_file_access_patterns_by_user AS (
    SELECT 
    e.src AS id,
    e.src AS user_join_key,
    MIN(e.event_ts) AS min_ts,
    '{most_recent_ts_in_batch}'::TIMESTAMP AS max_ts,
    datediff('{most_recent_ts_in_batch}'::TIMESTAMP, MIN(event_ts)) + 1 AS LogDurationInDays,
    -- Number of time written to sytem folder
    COUNT (DISTINCT CASE WHEN ss.id IS NOT NULL AND e.relationship IN ('file_written' , 'file_updated') THEN e.id END) AS NumTimesWrittenToSysFolder,
    COUNT (DISTINCT CASE WHEN ss.id IS NOT NULL AND e.relationship IN ('file_deleted') THEN e.id END) AS NumTimesDeletedSysFolder,
    COUNT (DISTINCT CASE WHEN ss.id IS NOT NULL AND e.relationship IN ('file_accessed') THEN e.id END) AS NumTimesAccessedSysFolder,
    COUNT (DISTINCT CASE WHEN ss.id IS NULL AND e.relationship IN ('file_accessed') THEN e.id END) AS NumTimesAccessedNormalFolder,
    COUNT (DISTINCT CASE WHEN ss.id IS NULL AND e.relationship IN ('file_deleted') THEN e.id END) AS NumTimesDeletedNormalFolder
    FROM e AS e
    LEFT JOIN prod_gold_dim_system_tables ss ON e.dst = ss.id
    WHERE e.relationship LIKE ('file%')
    GROUP BY e.src
    ),


    full_aggregates_sharing_patterns AS (
    SELECT 
    e.src AS id,
    e.src AS user_join_key, 
    MIN(e.event_ts) AS min_ts,
      '{most_recent_ts_in_batch}'::TIMESTAMP AS max_ts,
      datediff('{most_recent_ts_in_batch}'::TIMESTAMP, MIN(e.event_ts)) + 1 AS LogDurationInDays,
    CASE WHEN COUNT(DISTINCT e2.id) > 0 THEN 1 ELSE 0 END AS has_reciprocal_accessed_shares,
    COUNT(DISTINCT e2.id) AS num_reciprocal_accessed_shares,
    CASE WHEN COUNT(DISTINCT CASE WHEN e.relationship = 'shared_link' THEN e.id END) > 0 THEN 1 ELSE 0 END AS HasSharedFiles,
    COUNT(DISTINCT CASE WHEN e.relationship = 'shared_link' THEN e.entity_sub_relationship END) AS NumUsersSharingWith,
    COUNT(DISTINCT CASE WHEN e.relationship = 'shared_link' THEN e.id END) AS NumFilesShared,
    COUNT(DISTINCT CASE WHEN e.relationship = 'shared_link' AND e.status != 'success' THEN e.id END) AS NumFailedFileShares,
    COUNT(DISTINCT CASE WHEN e.relationship = 'public_share_accessed' AND e.status != 'success' THEN e.id END) AS NumFailedShareReadAttempts
    FROM e AS e
    LEFT JOIN e AS e2 ON e.entity_sub_relationship = e2.dst 
                      AND e2.relationship = 'public_share_accessed'
                      AND e2.src = e.dst
    WHERE e.relationship IN ('shared_link', 'public_share_accessed')
    GROUP BY e.src),

  --Unitize login patterns to daily rates
  rolling_login_patterns AS (
    SELECT 
    *,
    TotalLoginAttempts/LogDurationInDays AS AvgLoginAttemptsPerDay,
    TotalFailedAttempts/LogDurationInDays AS AvgFailedAttemptsPerDay
    FROM rolling_login_stats_by_user
  ),

  --Unitize file access patterns to daily rates
  rolling_file_access_patterns AS (
    SELECT 
    *,
    -- Unitized Features
    NumTimesWrittenToSysFolder/LogDurationInDays AS NumTimesWrittenToSysFolderPerDay,
    NumTimesDeletedSysFolder/LogDurationInDays AS NumTimesDeletedSysFolderPerDay,
    NumTimesAccessedSysFolder/LogDurationInDays AS NumTimesAccessedSysFolderPerDay,
    NumTimesAccessedNormalFolder/LogDurationInDays AS NumTimesAccessedNormalFolderPerDay,
    NumTimesDeletedNormalFolder/LogDurationInDays AS NumTimesDeletedNormFolderPerDay
    FROM rolling_file_access_patterns_by_user

  ),

  --Unitize data sharing patterns to daily rates
  rolling_data_sharing_patterns AS (
    SELECT *,
    -- Unitized Features
    NumUsersSharingWith/LogDurationInDays AS NumUsersSharingWithPerDay,
    NumFilesShared/LogDurationInDays AS NumFilesSharedPerDay,
    NumFailedFileShares/LogDurationInDays AS NumFailedFileSharesPerDay,
    NumFailedShareReadAttempts/LogDurationInDays AS NumFailedShareReadAttemptsPerDay
    FROM full_aggregates_sharing_patterns
  )


  -- Final Dataframe of features that model expects
  SELECT 
  u.entity_id AS user_id,
  AvgLoginAttemptsPerDay,
  AvgFailedAttemptsPerDay,
  NumTimesWrittenToSysFolderPerDay,
  NumTimesDeletedSysFolderPerDay,
  NumTimesAccessedSysFolderPerDay,
  NumTimesAccessedNormalFolderPerDay,
  NumTimesDeletedNormFolderPerDay,
  HasSharedFiles,
  NumUsersSharingWithPerDay,
  NumFilesSharedPerDay,
  NumFailedFileSharesPerDay,
  NumFailedShareReadAttemptsPerDay
  FROM batch_users AS u 
  LEFT JOIN rolling_login_patterns AS logins ON logins.user_join_key = u.entity_id
  LEFT JOIN rolling_file_access_patterns AS file_access ON file_access.user_join_key = u.entity_id
  LEFT JOIN rolling_data_sharing_patterns AS data_sharing ON data_sharing.user_join_key = u.entity_id
  """)

  return df_user_features_rolling_in_batch.fillna(0)

# COMMAND ----------

# DBTITLE 1,Function To Generate Anomaly Predictions
def generate_predictions(input_feature_df) -> DataFrame: 
    
  # Load model as a Spark UDF. Override result_type if the model does not return double values.
  loaded_model = mlflow.pyfunc.spark_udf(spark, model_uri=model_uri, result_type='double')


  # Predict on a Spark DataFrame.
  predicted_batch_df = (input_feature_df
    .withColumn('predictions', loaded_model(struct(*map(col, [i for i in features_df.columns if i != "user_id"]))))
    .withColumn("prediction_timestamp", current_timestamp()) ## Get timestamp of predictions and track these over time (can be used as another feature input)
        )

  return predicted_batch_df

# COMMAND ----------

# DBTITLE 1,Pass in either batch or MicroBatch into Feature Generation + Inference Functions
features_df = generate_rolling_feature_df(active_silver_events)

# COMMAND ----------

display(features_df)

# COMMAND ----------

final_batch_predictions = generate_predictions(features_df)

# COMMAND ----------

# DBTITLE 1,Write out Batch to table / or message bus for use
(final_batch_predictions
 .write
 .format("delta")
 .mode("overwrite")
 .saveAsTable("prod_anomaly_predictions")
)
