# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Acquire Example UBA Data from Public Source

# COMMAND ----------

dbutils.widgets.dropdown("StartOver", "yes", ["yes", "no"])

start_over = dbutils.widgets.get("StartOver")

# COMMAND ----------

# MAGIC %sh mkdir -p /dbfs/FileStore/cyberworkshop/clue/

# COMMAND ----------

# MAGIC %sh wget https://zenodo.org/record/7119953/files/clue.zip -O /dbfs/FileStore/cyberworkshop/clue/clue.zip

# COMMAND ----------

# MAGIC %sh wget https://zenodo.org/record/7119953/files/clue.zip -O /local_disk0/tmp/clue.zip
# MAGIC %sh unzip /local_disk0/tmp/clue.zip -d /dbfs/FileStore/cyberworkshop/clue/

# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore/cyberworkshop/clue/")

# COMMAND ----------

from pyspark.sql.types import BooleanType, LongType, StringType, StructField, StructType

clue_schema = StructType(
    [
        StructField("id", LongType(), True),
        StructField("isLocalIP", BooleanType(), True),
        StructField(
            "params",
            StructType(
                [
                    StructField("errorCode", LongType(), True),
                    StructField("itemSource", LongType(), True),
                    StructField("itemType", StringType(), True),
                    StructField("newpath", StringType(), True),
                    StructField("oldpath", StringType(), True),
                    StructField("path", StringType(), True),
                    StructField("run", BooleanType(), True),
                    StructField("token", StringType(), True),
                    StructField("trigger", LongType(), True),
                    StructField("uidOwner", StringType(), True),
                    StructField("user", StringType(), True),
                ]
            ),
            True,
        ),
        StructField("role", StringType(), True),
        StructField("time", StringType(), True),
        StructField("type", StringType(), True),
        StructField("uid", StringType(), True),
        StructField("uidType", StringType(), True),
    ]
)


df_raw = (
    spark.read.format("json")
    .schema(clue_schema)
    .load("dbfs:/FileStore/cyberworkshop/clue/clue.json")
)


if start_over = "yes":
  spark.sql(f"DROP DATABASE IF EXISTS cyberworkshop CASCADE;")


spark.sql(f"CREATE DATABASE IF NOT EXISTS cyberworkshop")
spark.sql(f"USE cyberworkshop;")

df_raw.write.mode("overwrite").saveAsTable("cyberworkshop.raw_logs")

# COMMAND ----------

# DBTITLE 1,Optimizing table layout for query performance
# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(0) FROM raw_logs

# COMMAND ----------

# DBTITLE 1,Profile Table - Cardinality Profiling to determine possible partitions / indexing cols
# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC COUNT(DISTINCT id), 
# MAGIC COUNT(DISTINCT to_timestamp(time)),
# MAGIC COUNT(DISTINCT role), 
# MAGIC COUNT(DISTINCT type),
# MAGIC COUNT(DISTINCT uid),
# MAGIC COUNT(DISTINCT uidType)
# MAGIC  FROM raw_logs
# MAGIC
# MAGIC
# MAGIC  -- Good possible partition columns: role, uidType
# MAGIC  -- Good possible ZOPRDER columns: time, uid (why not id?) - depends on what we join / search on downstream

# COMMAND ----------

# DBTITLE 1,Clean up and Optimize to Prep for Clean Bronze Table
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE bronze_logs
# MAGIC PARTITIONED BY (entity_type, role) -- by customer / low cardinality level of isolation
# MAGIC AS
# MAGIC SELECT 
# MAGIC id, 
# MAGIC to_timestamp(time) AS event_ts,
# MAGIC role, 
# MAGIC type AS event_type,
# MAGIC uid AS entity_id,
# MAGIC uidType AS entity_type,
# MAGIC params AS metadata
# MAGIC  FROM raw_logs

# COMMAND ----------

# DBTITLE 1,What are some of these values?
# MAGIC %sql
# MAGIC
# MAGIC SELECT DISTINCT entity_type FROM bronze_logs;
# MAGIC
# MAGIC SELECT DISTINCT role FROM bronze_logs;
# MAGIC
# MAGIC -- Get unique users/entities
# MAGIC SELECT entity_id, COUNT(0) AS num_events FROM bronze_logs GROUP BY entity_id ORDER BY num_events DESC;
# MAGIC
# MAGIC -- Example: fast-coffee-ocelot-arbitrator 140,235 rows out of 50M

# COMMAND ----------

# DBTITLE 1,Show effect of Z-ordering - What if we want to analyze behavior of a particular entity or time range (or both)?
# MAGIC %sql
# MAGIC
# MAGIC /* Spark Plan Output
# MAGIC
# MAGIC ~ 48% skip proportion
# MAGIC ~ 9.9s total seconds of cpu time to scan table
# MAGIC ~ 37 millions rows passed to next stage past scan
# MAGIC
# MAGIC number of files pruned	59
# MAGIC number of files read	63
# MAGIC number of local scan tasks	18
# MAGIC number of non-local (rescheduled) scan tasks	0
# MAGIC number of parquet row groups read	61
# MAGIC number of partitions read	7
# MAGIC number of scanned columns	5
# MAGIC relative skew in total splits sizes distribution	0.0 B
# MAGIC rows output	37,291,130
# MAGIC scan time total (min, med, max)	9.9 s (49 ms, 474 ms, 1.5 s)
# MAGIC
# MAGIC */
# MAGIC SELECt
# MAGIC *
# MAGIC FROM bronze_logs
# MAGIC WHERE 
# MAGIC WHERE entity_id = 'fast-coffee-ocelot-arbitrator'
# MAGIC AND event_ts BETWEEN '2020-01-01 00:00:00'::timestamp AND '2023-01-01 00:00:00'::timestamp
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Look at the Spark plan for the above select... What do we notice?
# MAGIC
# MAGIC 1. Number of filtered files?
# MAGIC From Spark UI
# MAGIC number of files pruned	59
# MAGIC number of files read	63
# MAGIC
# MAGIC 2: Runtime? ~4s
# MAGIC
# MAGIC
# MAGIC ### Looks like our partitioning is working, but we are still reading a lot of data just to query one user... this adds up quick

# COMMAND ----------

# DBTITLE 1,Now ZORDER by common high cardinality filters / joins columns
# MAGIC %sql
# MAGIC
# MAGIC -- This operation is incremental, so its not such a big tax
# MAGIC
# MAGIC OPTIMIZE bronze_logs ZORDER BY (event_ts, entity_id);

# COMMAND ----------

# DBTITLE 1,Run the above search again! (POST ZORDER)
# MAGIC %sql
# MAGIC
# MAGIC -- What happened? It got slower, thats because our files sizes relative to our table size is too big. We need to generate more selective file sizes
# MAGIC /* Spark Plan Output
# MAGIC
# MAGIC ~ Skip Proportion 10%
# MAGIC ~ Total Scan Time 24.7 seconds
# MAGIC ~ 45 millions rows passed to next stage
# MAGIC
# MAGIC number of columns in the relation	7
# MAGIC number of files pruned	1
# MAGIC number of files read	9
# MAGIC number of local scan tasks	10
# MAGIC number of non-local (rescheduled) scan tasks	0
# MAGIC number of parquet row groups read	12
# MAGIC number of partitions read	7
# MAGIC number of scanned columns	5
# MAGIC relative skew in total splits sizes distribution	0.0 B
# MAGIC rows output	45,375,793
# MAGIC scan time total (min, med, max)	24.7 s (66 ms, 3.2 s, 3.8 s)
# MAGIC
# MAGIC */ 
# MAGIC SELECt
# MAGIC *
# MAGIC FROM bronze_logs
# MAGIC WHERE 
# MAGIC WHERE entity_id = 'fast-coffee-ocelot-arbitrator'
# MAGIC AND event_ts BETWEEN '2020-01-01 00:00:00'::timestamp AND '2023-01-01 00:00:00'::timestamp
# MAGIC

# COMMAND ----------

# DBTITLE 1,Can we do better? What if we small our files smaller? 
# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE bronze_logs SET TBLPROPERTIES ('delta.targetFileSize' = '16mb');
# MAGIC
# MAGIC CREATE OR REPLACE TABLE bronze_logs_search
# MAGIC TBLPROPERTIES ('delta.targetFileSize' = '16mb')
# MAGIC AS
# MAGIC SELECT * FROM bronze_logs;
# MAGIC
# MAGIC
# MAGIC -- We went from a just 1 file to 60, that is better! 
# MAGIC OPTIMIZE bronze_logs_search ZORDER BY (event_ts, entity_id);
# MAGIC

# COMMAND ----------

# DBTITLE 1,Try again with smaller file sizes -- Look at spark plan for filtered files
# MAGIC %sql
# MAGIC
# MAGIC -- Much better!
# MAGIC
# MAGIC /* Spark Plan Output
# MAGIC
# MAGIC ~ 91% Skip Proportion 
# MAGIC ~ 2.3 total seconds of table scan time! 
# MAGIC ~ 1,778,395 millions rows passed to next stage, much more selective!
# MAGIC
# MAGIC
# MAGIC number of files pruned	55
# MAGIC number of files read	5
# MAGIC number of local scan tasks	3
# MAGIC number of non-local (rescheduled) scan tasks	0
# MAGIC number of parquet row groups read	3
# MAGIC number of scanned columns	7
# MAGIC relative skew in total splits sizes distribution	0.0 B
# MAGIC rows output	1,778,395
# MAGIC scan time total (min, med, max)	2.3 s (558 ms, 867 ms, 892 ms)
# MAGIC
# MAGIC */
# MAGIC SELECT
# MAGIC *
# MAGIC FROM bronze_logs_search
# MAGIC WHERE 
# MAGIC WHERE entity_id = 'fast-coffee-ocelot-arbitrator'
# MAGIC AND event_ts BETWEEN '2020-01-01 00:00:00'::timestamp AND '2023-01-01 00:00:00'::timestamp

# COMMAND ----------

# DBTITLE 1,Can we do EVEN BETTER? What can Photon do for us here? Predictive IO
spark.sql("USE SCHEMA cyberworkshop;")

# COMMAND ----------

# DBTITLE 1,Run with Photon and Show different in Query Plan
# MAGIC %sql
# MAGIC
# MAGIC /*
# MAGIC
# MAGIC Pruned Same Files:
# MAGIC files pruned	55
# MAGIC files read	5
# MAGIC
# MAGIC Adds additional data filters in the scan before going to next stage:
# MAGIC rows output	20,846 instead of >1MM, this DRASTICALLY increase efficiency of transformation queries in adding to vectorized execution
# MAGIC rows scanned	3,604,365
# MAGIC
# MAGIC */
# MAGIC SELECT
# MAGIC *
# MAGIC FROM bronze_logs_search
# MAGIC WHERE 
# MAGIC WHERE entity_id = 'fast-coffee-ocelot-arbitrator'
# MAGIC AND event_ts BETWEEN '2020-01-01 00:00:00'::timestamp AND '2023-01-01 00:00:00'::timestamp
