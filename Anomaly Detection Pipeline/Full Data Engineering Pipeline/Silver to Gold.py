# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Build Gold Feature Tables
# MAGIC
# MAGIC ### - Feature Tables come in two buckets: 
# MAGIC 1. Regular / Modelled Features - i.e. time series / aggregations (i.e. number of failed logins over time, num successful logins over time)
# MAGIC 2. Relational Graph - based features. (i.e. numer of reciprocal file shared (motif finding), populatary of file access activity)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Photon Key here with all these transformations and joins

# COMMAND ----------

spark.sql("""USE cyberworkshop;""")

# COMMAND ----------

# DBTITLE 1,1. Modeled User Entity Features
# MAGIC %sql
# MAGIC
# MAGIC -- Number of failed logins over last 7, 14, 30 days (momentum of failures) relative to a given event we want to predict on
# MAGIC
# MAGIC --WITH current_event_batch
# MAGIC SELECT 
# MAGIC date_trunc('month', event_ts) AS event_month, COUNT(0) AS event_count
# MAGIC FROM prod_silver_events
# MAGIC GROUP BY date_trunc('month', event_ts) 
# MAGIC ORDER BY event_month DESC;

# COMMAND ----------

# DBTITLE 1,So lets pretend 2022-09 is our new batch, lets pick the most recent hour to simulate a batch
# MAGIC %md

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Number of failed logins over last 7, 14, 30 days (momentum of failures) relative to a given event we want to predict on
# MAGIC
# MAGIC --WITH current_event_batch
# MAGIC SELECT 
# MAGIC date_trunc('day', event_ts) AS event_month, COUNT(0) AS event_count
# MAGIC FROM prod_silver_events
# MAGIC WHERE date_trunc('month', event_ts) = '2022-09-28T00:00:00.000+0000' 
# MAGIC GROUP BY date_trunc('day', event_ts) 
# MAGIC ORDER BY event_month DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT  (current_timestamp()::long - 7*24*3600)::timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## These are user level features, so aggregate at the user level (like avg login fails per day * 30 days for estimated rolling average) - no need to complex join for historical features

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Then for inference on the much more narrow / smaller input batch, we can roll up features at that level at run time (i.e. doing range joins to create features for 3000 users per hour instead of 500k users all at once creating a MUCH bigger cartisian product)

# COMMAND ----------

# DBTITLE 1,Feature 1: Login Attempt and Failure Rate
# MAGIC %sql
# MAGIC
# MAGIC --- Generating Historical Avg Feature Sets for Training
# MAGIC
# MAGIC CREATE OR REPLACE TABLE prod_user_features_login_frequency AS 
# MAGIC
# MAGIC WITH raw_login_stats_by_user AS (
# MAGIC SELECT
# MAGIC entity_id,
# MAGIC MIN(event_ts) AS min_ts,
# MAGIC MAX(event_ts) AS max_tx,
# MAGIC datediff(MAX(event_ts), MIN(event_ts)) + 1 AS LogDurationInDays,
# MAGIC COUNT(CASE WHEN event_type = 'login_failed' THEN id ELSE NULL END) AS TotalFailedAttempts,
# MAGIC COUNT(CASE WHEN event_type = 'login_attempt' THEN id ELSE NULL END) AS TotalLoginAttempts
# MAGIC FROM prod_silver_events AS curr
# MAGIC WHERE entity_type = 'name' 
# MAGIC AND event_type IN ('login_failed', 'login_succeeded', 'login_attempt')
# MAGIC GROUP BY entity_id
# MAGIC
# MAGIC )
# MAGIC
# MAGIC SELECT 
# MAGIC *,
# MAGIC TotalLoginAttempts/LogDurationInDays AS AvgLoginAttempsPerDay,
# MAGIC TotalFailedAttempts/LogDurationInDays AS AvgFailedAttemptsPerDay
# MAGIC FROM raw_login_stats_by_user;
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Generating Features Relative to moment in time
# MAGIC %sql
# MAGIC
# MAGIC -- What happens when we materialize the table
# MAGIC CREATE OR REPLACE TABLE range_temp
# MAGIC AS
# MAGIC SELECT id AS hist_id, entity_id, event_ts FROM prod_silver_events 
# MAGIC          WHERE entity_type = 'name'
# MAGIC          AND event_type = 'login_failed';
# MAGIC
# MAGIC
# MAGIC SELECT 
# MAGIC curr.event_ts,
# MAGIC curr.entity_id,
# MAGIC curr.id,
# MAGIC curr.entity_type,
# MAGIC COUNT(hist.hist_id) AS num_failed_logins_rolling_7_daya
# MAGIC FROM prod_silver_events AS curr
# MAGIC LEFT JOIN range_temp AS hist ON curr.entity_id = hist.entity_id 
# MAGIC           -- This kicks the join out of Photon -- Photon makes joins MUCH faster
# MAGIC            -- AND hist.event_ts >= (curr.event_ts - INTERVAL 7 DAYS) -- using spark SortMergeJoin
# MAGIC             AND hist.event_ts >= (curr.event_ts::long - 1*24*3600)::timestamp -- now uses shuffle hash join in Photon
# MAGIC WHERE entity_type = 'name' 
# MAGIC AND event_type IN ('login_attempt')
# MAGIC
# MAGIC GROUP BY
# MAGIC curr.event_ts,
# MAGIC curr.entity_id,
# MAGIC curr.id,
# MAGIC curr.entity_type

# COMMAND ----------

from graphframes.graphframe import *
from pyspark.sql.functions import *

v = spark.table("prod_nodes")
e = spark.table("prod_edges")
g = GraphFrame(v, e)


## Number of times user shared something
num_times_user_shared_something = g.outDegrees
#display(g.outDegrees)

## Number of times user received a share of something
num_times_user_received_something = g.inDegrees

# COMMAND ----------

display(g.edges)

# COMMAND ----------

# DBTITLE 1,Feature 2: Something about file writing behavior from graph


# COMMAND ----------

# DBTITLE 1,Feature 3: Something about object sharing patterns from graph

