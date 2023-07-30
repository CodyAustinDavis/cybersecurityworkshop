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
# MAGIC 1. Sharing 
# MAGIC 2. Reading/writing
# MAGIC 3. Logins

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

  print("Dropping and reloading nodes...")
  spark.sql("DROP TABLE IF EXISTS prod_silver_nodes")

  print("Dropping and reloading edges...")
  spark.sql("DROP TABLE IF EXISTS prod_silver_edges")

  print("Dropping and reloading silver_events")
  spark.sql("DROP TABLE IF EXISTS prod_silver_events")

# COMMAND ----------

# DBTITLE 1,Define Event, Nodes and Edges Tables DDL
# MAGIC %sql
# MAGIC
# MAGIC -- Liquid Clustering!!
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS prod_silver_nodes
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
# MAGIC CREATE OR REPLACE TABLE prod_silver_edges
# MAGIC (
# MAGIC   id STRING,
# MAGIC   event_ts TIMESTAMP,
# MAGIC   relationship STRING,
# MAGIC   src STRING,
# MAGIC   dst STRING,
# MAGIC   entity_sub_relationship STRING,
# MAGIC   item_type_shared STRING,
# MAGIC   status STRING,
# MAGIC   update_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
# MAGIC )
# MAGIC PARTITIONED BY (relationship)
# MAGIC TBLPROPERTIES ('delta.targetFileSize' = '16mb', 'delta.feature.allowColumnDefaults' = 'supported');
# MAGIC
# MAGIC --OPTIMIZE prod_edges ZORDER BY (src, dst, event_ts);
# MAGIC
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
bronze_df = spark.readStream.table("prod_bronze_streaming_logs")

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
      CONCAT('share_obj_', metadata.itemSource) AS id,
      CONCAT('share_obj_', metadata.itemSource) AS name,
      CONCAT('share_obj_', metadata.itemType) AS entity_type,
      null AS entity_role
      FROM new_events
      WHERE entity_type = 'name'
      AND event_type IN ('shared_link', 'public_share_accessed')
      AND metadata.itemSource IS NOT NULL AND metadata.itemType IS NOT NULL

      UNION 
      
      -- The user node
      SELECT DISTINCT 
      CONCAT('user_', entity_id) AS id,
      CONCAT('user_', entity_id) AS name,
      'user' AS entity_type,
      role AS entity_role
      FROM new_events
      WHERE entity_type = 'name'
      AND entity_id IS NOT NULL AND entity_type IS NOT NULL

      UNION 

      -- File root folder node
      SELECT DISTINCT
      CONCAT('root_folder_', CASE WHEN length(split(metadata.path, "/")[0]) < 1 THEN  split(metadata.path, "/")[1] ELSE split(metadata.path, "/")[0] END) AS id,
      CONCAT('root_folder_', CASE WHEN length(split(metadata.path, "/")[0]) < 1 THEN  split(metadata.path, "/")[1] ELSE split(metadata.path, "/")[0] END) AS name,
      'root_folder' AS entity_type,
      NULL AS entity_role
      FROM new_events
      WHERE entity_type = 'name'
      AND event_type IN ('file_accessed', 'file_written', 'file_updated', 'file_created', 'file_deleted')
      AND length(metadata.path) > 1
      )

    MERGE INTO prod_silver_nodes AS target 
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
    event_type AS relationship,
    CONCAT('user_', metadata.uidOwner) AS src,
    CONCAT('share_obj_', metadata.itemSource) AS dst, -- user --> (shared to user) --> share_object
    CONCAT('user_', metadata.token) AS entity_sub_relationship,
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
    event_type AS relationship,
    CONCAT('share_obj_', metadata.itemSource) AS src,
    CONCAT('user_', metadata.token) AS dst,
    CONCAT('user_', metadata.uidOwner) AS entity_sub_relationship,
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
    event_type AS relationship,
    CONCAT('user_', entity_id) AS src,
    CONCAT('root_folder_', CASE WHEN length(split(metadata.path, "/")[0]) < 1 THEN  split(metadata.path, "/")[1] ELSE split(metadata.path, "/")[0] END)  AS dst,
    metadata.path AS entity_sub_relationship,
    'local_file' AS item_type_shared,
    CASE WHEN metadata.errorCode IS NULL OR metadata.errorCode = '200' THEN 'success' ELSE 'failed' END AS status 
    FROM new_events
    WHERE entity_type = 'name'
    AND event_type IN ('file_accessed', 'file_written', 'file_updated', 'file_created', 'file_deleted')
    AND length(metadata.path) > 1

    )

    MERGE INTO prod_silver_edges AS target 
    USING (SELECT *, current_timestamp() AS update_timestamp FROM new_edges) AS source
    ON source.id = target.id
    AND source.event_ts = target.event_ts
    AND source.relationship = target.relationship
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
  spark_batch.sql("""OPTIMIZE prod_silver_nodes ZORDER BY (id, entity_role);""")
  spark_batch.sql("""OPTIMIZE prod_silver_edges ZORDER BY (src, dst, event_ts);""")
  spark_batch.sql("""OPTIMIZE prod_silver_events ZORDER BY (entity_type, event_ts, entity_id);""")

  ## Show what this looks like for Liquid clusters as well - nothing

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
