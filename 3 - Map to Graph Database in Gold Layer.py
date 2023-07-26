# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Take optimized ingest tables and build features, run analytics on Graph data model
# MAGIC
# MAGIC 1. Create incremental graph data model
# MAGIC 2. Develop Features for prediction
# MAGIC 3. Benchmarks Performance of GraphX, Graphframes (with and without Photon)
# MAGIC

# COMMAND ----------

spark.sql(f"USE cyberworkshop;")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT entity_type, event_type, COUNT(0) AS events FROM bronze_logs_search 
# MAGIC --WHERE entity_type = 'name' 
# MAGIC GROUP BY entity_type, event_type ORDER BY  events DESC

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- 'shared_link' can be a relationship
# MAGIC
# MAGIC
# MAGIC -- Features: 
# MAGIC -- User num events that were the same in last 1, 7, 30 days
# MAGIC SELECT 
# MAGIC *
# MAGIC FROM bronze_logs_search
# MAGIC WHERE entity_type = 'name'
# MAGIC AND event_type IN ('login_attempt', 'login_failed', 'login_successful', 'user_became_groupadmin', 'user_added_to_group', 'command_executed', 'admin_settings_changed', 'password_changed', 'file_accessed', 'file_written', 'file_updated', 'file_created', 'file_deleted');

# COMMAND ----------

# DBTITLE 1,Share Popularity Feature: Build Graph of Users Who Share Links with Each Other
# MAGIC %sql
# MAGIC
# MAGIC -- 'shared_link' can be a relationship
# MAGIC
# MAGIC
# MAGIC -- Features: 
# MAGIC -- User num events that were the same in last 1, 7, 30 days
# MAGIC
# MAGIC -- Build Nodes Graph for link sharing
# MAGIC
# MAGIC --!! These can be built incrementally using MERGE operations! - makes for really efficient upkeep and querying of graph data sets
# MAGIC
# MAGIC CREATE OR REPLACE TABLE nodes
# MAGIC TBLPROPERTIES ('delta.targetFileSize' = '16mb')
# MAGIC AS 
# MAGIC SELECT 
# MAGIC DISTINCT entity_id AS id,
# MAGIC role AS entity_role,
# MAGIC entity_type AS entity_type -- user/ipaddress
# MAGIC FROM bronze_logs_search;
# MAGIC
# MAGIC
# MAGIC CREATE OR REPLACE TABLE edges
# MAGIC TBLPROPERTIES ('delta.targetFileSize' = '16mb')
# MAGIC AS 
# MAGIC SELECT 
# MAGIC DISTINCT id AS event_id,
# MAGIC event_ts,
# MAGIC event_type,
# MAGIC metadata.uidOwner AS src,
# MAGIC metadata.token AS dst,
# MAGIC metadata.itemSource AS item_shared,
# MAGIC metadata.itemType AS item_type_shared,
# MAGIC metadata.errorCode AS status
# MAGIC FROM bronze_logs_search
# MAGIC WHERE entity_type = 'name'
# MAGIC AND event_type IN ('shared_link', 'public_share_accessed');

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC OPTIMIZE nodes ZORDER BY (id);
# MAGIC OPTIMIZE edges ZORDER BY (event_ts, src, dst);

# COMMAND ----------

from graphframes.graphframe import *
from pyspark.sql.functions import *

v = spark.table("nodes")
e = spark.table("edges")
g = GraphFrame(v, e)


## Number of times user shared something
num_times_user_shared_something = g.outDegrees
#display(g.outDegrees)

## Number of times user received a share of something
num_times_user_received_something = g.inDegrees

# COMMAND ----------

display(num_times_user_shared_something.orderBy(desc("outDegree")))

# COMMAND ----------

# DBTITLE 1,Get Role Distribution of Users
role_user_distribution = g.vertices.groupBy(col("user_role")).agg(count(col("id")).alias("number_of_users"))

display(role_user_distribution.orderBy(desc("number_of_users")))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Graph Features that could be used to identify anomalous events or strange user behavior patterns
# MAGIC
# MAGIC 1. Number of reciprocal shares (i.e. indicates a potentially more trustworthy entity) - but also if this entity is comprimised, represents a threat path
# MAGIC 2. Also make files / shares node types. These can be read from multiple users and written to multiple users. This way we can identify the actual path of a given file
# MAGIC 3. Identify files shared more than 4 times with
# MAGIC 4. Find users in a share 'click' that is isolated (< 3 nodes) (connected components)
# MAGIC 5. Identify high risk files to write to with pageRank - this indicates a shared file that shouldnt be written to

# COMMAND ----------

# DBTITLE 1,Standard Features on time series data
# MAGIC %md
# MAGIC
# MAGIC ## Standard Features Using Tempo and Normal Aggregations
# MAGIC
# MAGIC 1. Rolling logins, rolling activity data
