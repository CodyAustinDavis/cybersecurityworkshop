# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## RDD vs Dataframe benchmarking for dataframe native operations and customer operations
# MAGIC
# MAGIC
# MAGIC 1. Dataframe vs RDD for operations with dataframe native APIs
# MAGIC 2. Dataframe vs RDD for custom udfs: python, scala

# COMMAND ----------

display(dbutils.fs.ls('/databricks-datasets'))

# COMMAND ----------

# DBTITLE 1,Load a test data set
dbutils.fs.ls('dbfs:/databricks-datasets/sample_logs/')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Operations to Benchmark: read in csv data, split, clean, and perform 2 group by / sum aggregations on the following:
# MAGIC
# MAGIC 1. RDD - Scala (We ned python will be much slower) Rdd vs Rdd so we dont benchmark this
# MAGIC 2. Dataframe - Scala
# MAGIC 3. Dataframe - Python

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Question: Do RDDs utilize Photon? NO

# COMMAND ----------

# DBTITLE 1,Scale this by 100x  for benchmarking
print(spark.read.text("dbfs:/databricks-datasets/sample_logs/*").count())

# COMMAND ----------

# DBTITLE 1,Scala RDD standard functions
# MAGIC %scala 
# MAGIC import org.apache.spark.sql.types._;
# MAGIC import org.apache.spark.sql.functions._;
# MAGIC
# MAGIC
# MAGIC val rawRDD = spark.sparkContext.textFile("dbfs:/databricks-datasets/sample_logs/*");
# MAGIC
# MAGIC // define schema
# MAGIC case class LogData(ip: String, mac: String, userId: String, timestampRaw:String, timezoneRow: String, apiCall: String, status: Int, code: Int)
# MAGIC
# MAGIC // Load dataset and apply the schema / clean rows
# MAGIC
# MAGIC val logDataRDD = rawRDD.map( line => {
# MAGIC
# MAGIC   val fields = line.split(" (?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
# MAGIC
# MAGIC   LogData(fields(0), fields(1), fields(2), fields(3), fields(4), fields(5), fields(6).toInt, fields(7).toInt)
# MAGIC
# MAGIC });
# MAGIC
# MAGIC // Function to clean timezone
# MAGIC def cleanTimezone(timezoneRaw : String) : String = {
# MAGIC   timezoneRaw.replace("]", "")
# MAGIC }
# MAGIC
# MAGIC
# MAGIC // Aggregate #1: Get API call count by ip
# MAGIC
# MAGIC val ipCountData = logDataRDD.map(m => (m.ip, 1))
# MAGIC
# MAGIC val ipCountAggregate = ipCountData.reduceByKey(_ + _)
# MAGIC
# MAGIC println("Aggregates for API calls by IP: ")
# MAGIC //ipCountAggregate.collect().foreach(println);
# MAGIC
# MAGIC // Aggregate #2: Get API call count by ip and status
# MAGIC
# MAGIC val ipStatusData = logDataRDD.map(m => ((m.ip, m.status), 1))
# MAGIC
# MAGIC val ipStatsCountAggregate = ipStatusData.reduceByKey(_ + _)
# MAGIC
# MAGIC println("Aggregates for API calls by IP and STATUS: ")
# MAGIC //ipStatsCountAggregate.collect().foreach(println);
# MAGIC
# MAGIC ipCountAggregate.saveAsTextFile("dbfs:/temp/workshop/rdd/scala/ip_count")
# MAGIC ipStatsCountAggregate.saveAsTextFile("dbfs:/temp/workshop/rdd/scala/ip_status_count")

# COMMAND ----------

# DBTITLE 1,Scala Standard Dataframe Functions
# MAGIC %scala 
# MAGIC import org.apache.spark.sql.types._;
# MAGIC import org.apache.spark.sql.functions._;
# MAGIC
# MAGIC
# MAGIC val df_1 = spark.read.option("sep", " ").option("quote", "\"").csv("dbfs:/databricks-datasets/sample_logs/");
# MAGIC
# MAGIC val df_raw = df_1.toDF("ip", "mac", "user_id", "timestamp_raw", "timezone_raw", "api_call", "status", "code");
# MAGIC
# MAGIC val df_cleaned = {df_raw.select(col("ip"), 
# MAGIC                   col("mac"), 
# MAGIC                   col("user_id"), 
# MAGIC                   col("timestamp_raw"), 
# MAGIC                   regexp_replace(df_raw("timezone_raw"), "]", "").alias("timezone"), 
# MAGIC                   col("api_call"), 
# MAGIC                   col("status").cast("integer").alias("status"), 
# MAGIC                   col("code").cast("integer").alias("code")
# MAGIC                   )             
# MAGIC };
# MAGIC
# MAGIC val df_agg_by_ip = df_cleaned.groupBy(col("ip")).agg(count(lit(0)).cast("string").alias("api_calls"));
# MAGIC
# MAGIC
# MAGIC val df_agg_by_ip_status = df_cleaned.groupBy(col("ip"), col("status")).agg(count(lit(0)).cast("string").alias("api_calls_by_status"));
# MAGIC
# MAGIC
# MAGIC df_agg_by_ip.write.mode("overwrite").csv("dbfs:/temp/workshop/df/scala/ip_count")
# MAGIC df_agg_by_ip_status.write.mode("overwrite").csv("dbfs:/temp/workshop/df/scala/ip_status_count")
# MAGIC

# COMMAND ----------

# DBTITLE 1,Python Standard Dataframe Functions
from pyspark.sql.functions import *

df_1 = spark.read.option("sep", " ").option("quote", "\"").csv('dbfs:/databricks-datasets/sample_logs/')

df_raw = df_1.toDF(*["ip", "mac", "user_id", "timestamp_raw", "timezone_raw", "api_call", "status", "code"])

df_cleaned = (df_raw
          .select(col("ip"), 
                  "mac", 
                  "user_id", 
                  "timestamp_raw", 
                  regexp_replace(col("timezone_raw"), "]", "").alias("timezone"), 
                  "api_call", 
                  col("status").cast("integer"), 
                  col("code").cast("integer")
                  )             
)

df_agg_by_ip_python = df_cleaned.groupBy(col("ip")).agg(count(lit(0)).alias("api_calls"))


df_agg_by_ip_status_python = df_cleaned.groupBy(col("ip"), col("status")).agg(count(lit(0)).alias("api_calls_by_status"))


df_agg_by_ip_python.write.mode("overwrite").csv("dbfs:/temp/workshop/df/python/ip_count")
df_agg_by_ip_status_python.write.mode("overwrite").csv("dbfs:/temp/workshop/df/python/ip_status_count")

