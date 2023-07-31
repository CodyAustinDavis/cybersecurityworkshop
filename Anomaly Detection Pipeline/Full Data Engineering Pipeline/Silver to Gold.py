# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Build Gold Aggregate Feature Tables for Historical Training
# MAGIC
# MAGIC <b> Tip: </b> You can build aggregates for users from scratch or just recompute the aggregates for only the users that are in an incoming batch inside the stream (or use Databricks Materialized Views :))
# MAGIC ### - Feature Tables come in two buckets: 
# MAGIC 1. Regular / Modelled Features - i.e. time series / aggregations (i.e. number of failed logins over time, num successful logins over time)
# MAGIC 2. Relational Graph - based features. (i.e. numer of reciprocal file shared (motif finding), populatary of file access activity)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### User Features: 
# MAGIC
# MAGIC ### Modelled: 
# MAGIC
# MAGIC - Login Attempt Rate (unit: daily rate)
# MAGIC - Login Failure Rate (unit: daily rate)
# MAGIC
# MAGIC
# MAGIC ### Graph
# MAGIC
# MAGIC - Share Object Rate per user (unit: daily rate) - outDegree of user --> shares rate
# MAGIC - Share Access Rate per user (unit: daily rate) - inDegree of user --> shares rate
# MAGIC - File Write Rate per user (unit: daily rate) - outDegree of user --> file write/update rate 
# MAGIC - File Deletion Rate per user (unit: daily rate) - outDegree of user --> deletion rate
# MAGIC - File Read Rate per user (unit: daily rate) - outDegree of user --> read rate
# MAGIC
# MAGIC ### Fancier Features
# MAGIC
# MAGIC - Combine features that indicate specific beahvior (motif findings): i.e. high degree of reciprocal data sharing ([a] -> [e] --> [b]; [b] --> [e2]--> [a]) + high number of file writes + high number of inDegree for a particular IP address (if there was an ip address node)
# MAGIC
# MAGIC - Graph features around the weights of a users connected nodes (i.e. average number of degrees of each of their total connections) - i.e. level of influence on the network

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Photon is key here with all these transformations and joins

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

# MAGIC %md
# MAGIC
# MAGIC ## Then for inference on the much more narrow / smaller input batch, we can roll up features at that level at run time (i.e. doing range joins to create features for 3000 users per hour instead of 500k users all at once creating a MUCH bigger cartisian product)

# COMMAND ----------

# DBTITLE 1,Feature 1 and 2: Login Attempt and Failure Rate
# MAGIC %sql
# MAGIC
# MAGIC --- Generating Historical Avg Feature Sets for Training
# MAGIC
# MAGIC CREATE OR REPLACE TABLE prod_gold_user_historical_features_logins AS 
# MAGIC
# MAGIC WITH raw_login_stats_by_user AS (
# MAGIC SELECT
# MAGIC entity_id,
# MAGIC MIN(event_ts) AS min_ts,
# MAGIC MAX(event_ts) AS max_ts,
# MAGIC datediff(MAX(event_ts), MIN(event_ts)) + 1 AS LogDurationInDays,
# MAGIC COUNT(CASE WHEN event_type = 'login_failed' THEN id ELSE NULL END) AS TotalFailedAttempts,
# MAGIC COUNT(CASE WHEN event_type = 'login_attempt' THEN id ELSE NULL END) AS TotalLoginAttempts
# MAGIC FROM prod_silver_events AS curr
# MAGIC WHERE entity_type = 'name' 
# MAGIC AND event_type IN ('login_failed', 'login_succeeded', 'login_attempt')
# MAGIC AND event_ts <= '2022-09-01T00:00:00.000+0000' -- simulate historical dataset, most recent month is reserved to do inference
# MAGIC GROUP BY entity_id
# MAGIC
# MAGIC )
# MAGIC
# MAGIC SELECT 
# MAGIC *,
# MAGIC CONCAT('user_', entity_id) AS user_join_key,
# MAGIC TotalLoginAttempts/LogDurationInDays AS AvgLoginAttemptsPerDay,
# MAGIC TotalFailedAttempts/LogDurationInDays AS AvgFailedAttemptsPerDay
# MAGIC FROM raw_login_stats_by_user;
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Build Some Graph Features
from graphframes.graphframe import *
from pyspark.sql.functions import *

v = spark.table("prod_silver_nodes")
e = spark.table("prod_silver_edges")

v.createOrReplaceTempView("v")
e.createOrReplaceTempView("e")

g = GraphFrame(v, e)

# COMMAND ----------

# DBTITLE 1,Profile Graph Events
# MAGIC %sql
# MAGIC
# MAGIC SELECT relationship, COUNT(0) AS events FROM e GROUP BY relationship ORDEr BY events DESC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Simulated Feature Set: Root folder access patterns 
# MAGIC
# MAGIC  - Assuming with our fake data, that the top 10 most commonly shared root folders are generic "system" folders in all computers. If a user is writing / updating / deleting these files frequently, that could be a feature we want to track for anomolous user behavior

# COMMAND ----------

# DBTITLE 1,Recording System Tables to flag write frequency to system tables
g_root_folders = g.filterEdges("relationship LIKE ('file%')").inDegrees

top_system_tables = g_root_folders.orderBy(desc("inDegree")).limit(10)

top_system_tables.write.format("delta").mode("overwrite").saveAsTable("prod_gold_dim_system_tables")

display(top_system_tables)

# COMMAND ----------

display(g.filterEdges("relationship LIKE ('file%')").edges)

# COMMAND ----------

# DBTITLE 1,System Table File Access Pattern Behavior
# MAGIC %sql
# MAGIC
# MAGIC -- Translate to unit rates (i.e. daily) to normalize historical data sources with rolling data sources
# MAGIC
# MAGIC CREATE OR REPLACE TABLE prod_gold_user_historical_features_file_access_patterns
# MAGIC AS 
# MAGIC WITH full_aggregates_file_access AS (
# MAGIC SELECT 
# MAGIC e.src AS id,
# MAGIC e.src AS user_join_key,
# MAGIC MIN(e.event_ts) AS min_ts,
# MAGIC MAX(e.event_ts) AS max_ts,
# MAGIC datediff(MAX(event_ts), MIN(event_ts)) + 1 AS LogDurationInDays,
# MAGIC -- Number of time written to sytem folder
# MAGIC COUNT (DISTINCT CASE WHEN ss.id IS NOT NULL AND e.relationship IN ('file_written' , 'file_updated') THEN e.id END) AS NumTimesWrittenToSysFolder,
# MAGIC COUNT (DISTINCT CASE WHEN ss.id IS NOT NULL AND e.relationship IN ('file_deleted') THEN e.id END) AS NumTimesDeletedSysFolder,
# MAGIC COUNT (DISTINCT CASE WHEN ss.id IS NOT NULL AND e.relationship IN ('file_accessed') THEN e.id END) AS NumTimesAccessedSysFolder,
# MAGIC COUNT (DISTINCT CASE WHEN ss.id IS NULL AND e.relationship IN ('file_accessed') THEN e.id END) AS NumTimesAccessedNormalFolder,
# MAGIC COUNT (DISTINCT CASE WHEN ss.id IS NULL AND e.relationship IN ('file_deleted') THEN e.id END) AS NumTimesDeletedNormalFolder
# MAGIC FROM e AS e
# MAGIC LEFT JOIN prod_gold_dim_system_tables ss ON e.dst = ss.id
# MAGIC WHERE e.relationship LIKE ('file%')
# MAGIC AND e.event_ts <= '2022-09-01T00:00:00.000+0000' -- simulate historical dataset, most recent month is reserved to do inference
# MAGIC GROUP BY e.src
# MAGIC )
# MAGIC SELECT 
# MAGIC *,
# MAGIC -- Unitized Features
# MAGIC NumTimesWrittenToSysFolder/LogDurationInDays AS NumTimesWrittenToSysFolderPerDay,
# MAGIC NumTimesDeletedSysFolder/LogDurationInDays AS NumTimesDeletedSysFolderPerDay,
# MAGIC NumTimesAccessedSysFolder/LogDurationInDays AS NumTimesAccessedSysFolderPerDay,
# MAGIC NumTimesAccessedNormalFolder/LogDurationInDays AS NumTimesAccessedNormalFolderPerDay,
# MAGIC NumTimesDeletedNormalFolder/LogDurationInDays AS NumTimesDeletedNormFolderPerDay
# MAGIC FROM full_aggregates_file_access

# COMMAND ----------

# DBTITLE 1,File Sharing Feature Set
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE prod_gold_user_historical_features_sharing_patterns
# MAGIC AS (
# MAGIC   
# MAGIC WITH full_aggregates_sharing_patterns AS (
# MAGIC SELECT 
# MAGIC e.src AS id,
# MAGIC e.src AS user_join_key, 
# MAGIC MIN(e.event_ts) AS min_ts,
# MAGIC MAX(e.event_ts) AS max_ts,
# MAGIC datediff(MAX(e.event_ts), MIN(e.event_ts)) + 1 AS LogDurationInDays,
# MAGIC CASE WHEN COUNT(DISTINCT e2.id) > 0 THEN 1 ELSE 0 END AS has_reciprocal_accessed_shares,
# MAGIC COUNT(DISTINCT e2.id) AS num_reciprocal_accessed_shares,
# MAGIC CASE WHEN COUNT(DISTINCT CASE WHEN e.relationship = 'shared_link' THEN e.id END) > 0 THEN 1 ELSE 0 END AS HasSharedFiles,
# MAGIC COUNT(DISTINCT CASE WHEN e.relationship = 'shared_link' THEN e.entity_sub_relationship END) AS NumUsersSharingWith,
# MAGIC COUNT(DISTINCT CASE WHEN e.relationship = 'shared_link' THEN e.id END) AS NumFilesShared,
# MAGIC COUNT(DISTINCT CASE WHEN e.relationship = 'shared_link' AND e.status != 'success' THEN e.id END) AS NumFailedFileShares,
# MAGIC COUNT(DISTINCT CASE WHEN e.relationship = 'public_share_accessed' AND e.status != 'success' THEN e.id END) AS NumFailedShareReadAttempts
# MAGIC FROM e AS e
# MAGIC LEFT JOIN e AS e2 ON e.entity_sub_relationship = e2.dst 
# MAGIC                   AND e2.relationship = 'public_share_accessed'
# MAGIC                   AND e2.src = e.dst
# MAGIC WHERE e.relationship IN ('shared_link', 'public_share_accessed')
# MAGIC AND e.event_ts <= '2022-09-01T00:00:00.000+0000' -- simulate historical dataset, most recent month is reserved to do inference
# MAGIC GROUP BY e.src)
# MAGIC
# MAGIC SELECT *,
# MAGIC -- Unitized Features
# MAGIC NumUsersSharingWith/LogDurationInDays AS NumUsersSharingWithPerDay,
# MAGIC NumFilesShared/LogDurationInDays AS NumFilesSharedPerDay,
# MAGIC NumFailedFileShares/LogDurationInDays AS NumFailedFileSharesPerDay,
# MAGIC NumFailedShareReadAttempts/LogDurationInDays AS NumFailedShareReadAttemptsPerDay
# MAGIC FROM full_aggregates_sharing_patterns
# MAGIC )

# COMMAND ----------

# DBTITLE 1,User Sharing Rates Graph Features - Graph Version of doing the above without all the joins
## entity_type = 'name'
##event_type = 'shared_link'

## Features:
# Number of users successfully shared
# Number of users failed shared (status != success)
# Number of reciporcal shared (motif) either 

num_files_shared_per_user = g.filterEdges("relationship IN ('shared_link')").outDegrees
num_file_shares_accessed_per_user = g.filterEdges("relationship IN ('public_share_accessed')").inDegrees
num_failed_shared_accessed_per_user = g.filterEdges("relationship IN ('public_share_accessed') AND status != 'success'").inDegrees
num_failed_shares_per_user = g.filterEdges("relationship IN ('shared_link') AND status != 'success'").inDegrees
num_share_actions_per_user = g.filterEdges("relationship IN ('shared_link', 'public_share_accessed')").degrees

#share_patterns = g.find("(a)-[e]->(b)")
display(g.find("(a)-[e]->(b); (b)-[e2]->(a)"))

# COMMAND ----------

# DBTITLE 1,Generate Other Features based on more nuanced graph algoritms
connected_components_checkpoint = f"dbfs:/FileStore/cyberworkshop/checkpoints/connected_components/"
sc.setCheckpointDir(connected_components_checkpoint)

entity_connected_components = g.connectedComponents()

display(entity_connected_components)
