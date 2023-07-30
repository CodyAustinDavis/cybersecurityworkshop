# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Raw Logs --> Bronze Incremental Delta Layer
# MAGIC
# MAGIC - This notebook assumes the the raw_logs table is continuously updated with append only data. For the demo, the data source is static but treated as a stream

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *

# COMMAND ----------

# DBTITLE 1,Define Parameter
dbutils.widgets.dropdown("StartOver", "yes", ["no", "yes"])

# COMMAND ----------

# DBTITLE 1,Get Parameter
start_over = dbutils.widgets.get("StartOver")


# COMMAND ----------

# DBTITLE 1,Define Checkpoint and Target Database
spark.sql(f"CREATE DATABASE IF NOT EXISTS cyberworkshop")
spark.sql(f"USE cyberworkshop;")

## Writing to managed tables only for simplicity

checkpoint_location = "dbfs:/FileStore/cyberworkshop/checkpoints/raw_to_bronze/"

if start_over == "yes":

  print(f"Staring over Raw --> Bronze Stream...")
  dbutils.fs.rm(checkpoint_location, recurse=True)

# COMMAND ----------

# DBTITLE 1,Define Readstream

df_raw = (spark.readStream.table("cyberworkshop.raw_logs")
)

# COMMAND ----------

# DBTITLE 1,Basic Data Transformations on Streaming Data Frame
df_clean = (df_raw.select("id", 
                          to_timestamp("time").alias("event_ts"),
                          "role",
                          col("type").alias("event_type"),
                          col("uid").alias("entity_id"),
                          col("uidType").alias("entity_type"),
                          col("params").alias("metadata")
                          ))
                          

# COMMAND ----------

# DBTITLE 1,Write Stream to Target Delta Table
(df_clean
 .writeStream
 .partitionBy("entity_type", "role")
 .option("checkpointLocation", checkpoint_location)
 .trigger(once=True) ## availableNow=True, processingTime = "X mins/sec/hour"
 .toTable("cyberworkshop.prod_bronze_streaming_logs")
 .awaitTermination() ## Do not run downstream commands until stream finishes
)

# COMMAND ----------

# DBTITLE 1,Optimize Table once Stream is done
spark.sql("""ALTER TABLE cyberworkshop.prod_bronze_streaming_logs SET TBLPROPERTIES ('delta.targetFileSize' = '16mb');""")
spark.sql("""OPTIMIZE cyberworkshop.prod_bronze_streaming_logs ZORDER BY (event_ts, entity_id);""")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE HISTORY cyberworkshop.prod_bronze_streaming_logs
