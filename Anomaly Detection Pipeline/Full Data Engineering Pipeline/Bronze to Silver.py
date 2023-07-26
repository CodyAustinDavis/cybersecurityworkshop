# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Bronze --> Silver Log Pipeline
# MAGIC
# MAGIC  - This pipeline will model the data into normal dimensions entities and graph relationships in a streaming pipeline

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Graph Entities
# MAGIC
# MAGIC #### Nodes (objects): 
# MAGIC
# MAGIC 1. Users
# MAGIC 2. Files/Folder Shares
# MAGIC 3. File Paths (local) Roots
# MAGIC
# MAGIC
# MAGIC #### Edges (relationships / actions): 
# MAGIC 1. Users
# MAGIC 2. File Paths (root locations only)
# MAGIC 3. Shares

# COMMAND ----------

# DBTITLE 1,Define Parameter
dbutils.widgets.dropdown("StartOver", "yes", ["no", "yes"])

# COMMAND ----------

# DBTITLE 1,Get Parameter
start_over = dbutils.widgets.get("StartOver")

# COMMAND ----------

# DBTITLE 1,Use Database for pipeline
spark.sql("USE cyberworkshop;")

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS cyberworkshop")
spark.sql(f"USE cyberworkshop;")

## Writing to managed tables only for simplicity

checkpoint_location = "dbfs:/FileStore/cyberworkshop/checkpoints/bronze_to_silver/"

if start_over == "yes":

  print(f"Staring over Bronze --> Silver Streams...")
  dbutils.fs.rm(checkpoint_location, recurse=True)

# COMMAND ----------

# DBTITLE 1,Define Event, Nodes and Edges Tables DDL
# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS prod_nodes
# MAGIC (id STRING,
# MAGIC name STRING,
# MAGIC entity_type STRING,
# MAGIC entity_role STRING,
# MAGIC update_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
# MAGIC )
# MAGIC PARTITIONED BY (entity_type)
# MAGIC TBLPROPERTIES ('delta.targetFileSize' = '16mb', 'delta.feature.allowColumnDefaults' = 'supported')
# MAGIC ;
# MAGIC
# MAGIC --OPTIMIZE prod_nodes ZORDER BY (id, entity_role);
# MAGIC
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS prod_edges
# MAGIC (
# MAGIC   id STRING,
# MAGIC   event_ts TIMESTAMP,
# MAGIC   event_type STRING,
# MAGIC   src STRING,
# MAGIC   dst STRING,
# MAGIC   status STRING,
# MAGIC   item_shared STRING,
# MAGIC   item_type_shared STRING,
# MAGIC   update_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
# MAGIC )
# MAGIC PARTITIONED BY (event_type)
# MAGIC TBLPROPERTIES ('delta.targetFileSize' = '16mb', 'delta.feature.allowColumnDefaults' = 'supported');
# MAGIC
# MAGIC --OPTIMIZE prod_edges ZORDER BY (src, dst, event_ts);
# MAGIC
# MAGIC /*
# MAGIC
# MAGIC (
# MAGIC   id LONG,
# MAGIC   event_ts TIMESTAMP,
# MAGIC   role STRING,
# MAGIC   event_type STRING,
# MAGIC   entity_id STRING,
# MAGIC   entity_type STRING,
# MAGIC   metadata STRUCT<errorCode:long, itemSource:long, itemType:string, newpath:string,voldpath:string, path:string, run:boolean, token:string, trigger:long, uidOwner:string, user:string>,
# MAGIC   update_timestamp TIMESTAMP
# MAGIC )
# MAGIC
# MAGIC
# MAGIC */
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS prod_silver_events
# MAGIC TBLPROPERTIES ('delta.targetFileSize' = '16mb', 'delta.feature.allowColumnDefaults' = 'supported')
# MAGIC AS
# MAGIC SELECT * FROM prod_streaming_bronze_logs WHERE 1=2
# MAGIC ;
# MAGIC --OPTIMIZE prod_silver_events ZORDER BY (entity_type, event_ts, entity_id);
# MAGIC

# COMMAND ----------

# DBTITLE 1,Read Stream for New Data
bronze_df = spark.readStream.table("prod_streaming_bronze_logs")

# COMMAND ----------

# DBTITLE 1,Microbatch Merge Logic
def controllerFunction(input_df, id):

  input_df.createOrReplaceTempView("new_events")

  spark_batch = input_df._jdf.sparkSession()


  ### Load Nodes Incrementally
  spark_batch.sql("""
                  
    ----- Nodes -----
    WITH new_nodes AS (
      -- The Share Object Node
      SELECT DISTINCT 
      metadata.itemSource AS id,
      metadata.itemSource AS name,
      CONCAT('share_object_', metadata.itemType) AS entity_type,
      null AS entity_role
      FROM new_events
      WHERE entity_type = 'name'
      AND event_type IN ('shared_link', 'public_share_accessed')
      AND metadata.itemSource IS NOT NULL AND metadata.itemType IS NOT NULL

      UNION 
      
      -- The user node
      SELECT DISTINCT 
      entity_id AS id,
      entity_id AS name,
      entity_type AS entity_type,
      role AS entity_role
      FROM new_events
      WHERE entity_type = 'name'
      AND entity_id IS NOT NULL AND entity_type IS NOT NULL

      UNION 

      -- File root folder node
      SELECT DISTINCT
      CASE WHEN length(split(metadata.path, "/")[0]) < 1 THEN  split(metadata.path, "/")[1] ELSE split(metadata.path, "/")[0] END AS id,
      CASE WHEN length(split(metadata.path, "/")[0]) < 1 THEN  split(metadata.path, "/")[1] ELSE split(metadata.path, "/")[0] END AS name,
      'root_folder' AS entity_type,
      NULL AS entity_role
      FROM new_events
      WHERE entity_type = 'name'
      AND event_type IN ('file_accessed', 'file_written', 'file_updated', 'file_created', 'file_deleted')
      AND length(metadata.path) > 1
      )

    MERGE INTO prod_nodes AS target 
    USING (SELECT *, current_timestamp() AS update_timestamp FROM new_nodes) AS source
    ON source.id = target.id
    AND source.name = target.name
    AND source.entity_type = target.entity_type
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *;

    """)
  
  ## Load edges incrementally
  spark_batch.sql("""

    ---- Edges -----
    WITH new_edges AS (

    -- For user sharing a file
    SELECT 
    DISTINCT id AS id,
    event_ts,
    event_type,
    metadata.uidOwner AS src,
    metadata.token AS dst,
    metadata.itemSource AS item_shared,
    metadata.itemType AS item_type_shared,
    CASE WHEN metadata.errorCode IS NULL OR metadata.errorCode = '200' THEN 'success' ELSE 'failed' END AS status
    FROM new_events
    WHERE entity_type = 'name'
    AND event_type = 'shared_link'

    UNION

    -- user accessing a file shared by a user
    SELECT 
    DISTINCT id AS id,
    event_ts,
    event_type,
    metadata.token AS src,
    metadata.uidOwner AS dst,
    metadata.itemSource AS item_shared,
    metadata.itemType AS item_type_shared,
    CASE WHEN metadata.errorCode IS NULL OR metadata.errorCode = '200' THEN 'success' ELSE 'failed' END AS status
    FROM new_events
    WHERE entity_type = 'name'
    AND event_type = 'public_share_accessed'

    UNION 
    -- File action edge
    SELECT 
    DISTINCT 
    id AS id,
    event_ts,
    event_type,
    entity_id AS src,
    CASE WHEN length(split(metadata.path, "/")[0]) < 1 THEN  split(metadata.path, "/")[1] ELSE split(metadata.path, "/")[0] END  AS dst,
    metadata.path AS item_shared,
    'local_file' AS item_type_shared,
    CASE WHEN metadata.errorCode IS NULL OR metadata.errorCode = '200' THEN 'success' ELSE 'failed' END AS status 
    FROM new_events
    WHERE entity_type = 'name'
    AND event_type IN ('file_accessed', 'file_written', 'file_updated', 'file_created', 'file_deleted')
    AND length(metadata.path) > 1

    )

    MERGE INTO prod_edges AS target 
    USING (SELECT *, current_timestamp() AS update_timestamp FROM new_edges) AS source
    ON source.id = target.id
    AND source.event_ts = target.event_ts
    AND source.event_type = target.event_type
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *;

  """)


  ### Now Load the events incrementally
  spark_batch.sql("""
    MERGE INTO prod_silver_events AS target
    USING (SELECT * FROM new_events) AS source
    ON source.entity_id = target.entity_id
    AND source.event_ts = target.event_ts 
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *;
                  """)
  


  ### Optimize tables after merges
  spark_batch.sql("""OPTIMIZE prod_nodes ZORDER BY (id, entity_role);""")
  spark_batch.sql("""OPTIMIZE prod_edges ZORDER BY (src, dst, event_ts);""")
  spark_batch.sql("""OPTIMIZE prod_silver_events ZORDER BY (entity_type, event_ts, entity_id);""")


  print("Pipeline Refresh Graph and Event Model Complete!")

  return 

# COMMAND ----------

# DBTITLE 1,Write Stream - Passing stream into batch processor function
(bronze_df
 .writeStream
 .option("checkpointLocation", checkpoint_location)
 .trigger(once=True) ## processingTime=X INTERVAL, availableNow=True
 .foreachBatch(controllerFunction)
 .start()
)
