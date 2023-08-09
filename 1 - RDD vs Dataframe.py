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

# DBTITLE 1,Records in File
print(spark.read.text("dbfs:/databricks-datasets/sample_logs/*").count())

# COMMAND ----------

# MAGIC %scala 
# MAGIC
# MAGIC val rawRDD = spark.sparkContext.textFile("dbfs:/databricks-datasets/sample_logs/*");
# MAGIC val biggerRDD = spark.sparkContext.union(Seq(rawRDD, rawRDD, rawRDD, rawRDD, rawRDD));
# MAGIC

# COMMAND ----------

# DBTITLE 1,Scala RDD standard functions
# MAGIC %scala 
# MAGIC import org.apache.spark.sql.types._;
# MAGIC import org.apache.spark.sql.functions._;
# MAGIC
# MAGIC // duplicate 10 times to make it a little bigger
# MAGIC val rawRDD = spark.sparkContext.textFile("dbfs:/databricks-datasets/sample_logs/*");
# MAGIC val biggerRDD = spark.sparkContext.union(Seq(rawRDD, rawRDD, rawRDD, rawRDD, rawRDD, rawRDD, rawRDD, rawRDD, rawRDD, rawRDD));
# MAGIC
# MAGIC // define schema
# MAGIC case class LogData(ip: String, mac: String, userId: String, timestampRaw:String, timezoneRow: String, apiCall: String, status: Int, code: Int)
# MAGIC
# MAGIC // Load dataset and apply the schema / clean rows
# MAGIC
# MAGIC val logDataRDD = biggerRDD.map( line => {
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
# MAGIC ipCountAggregate.collect().foreach(println);
# MAGIC
# MAGIC // Aggregate #2: Get API call count by ip and status
# MAGIC
# MAGIC val ipStatusData = logDataRDD.map(m => ((m.ip, m.status), 1))
# MAGIC
# MAGIC val ipStatsCountAggregate = ipStatusData.reduceByKey(_ + _)
# MAGIC
# MAGIC // If already exists, need to write more code just to delete files where with dataframes, you can just add .mode("overwrite")
# MAGIC
# MAGIC try {
# MAGIC   ipCountAggregate.saveAsTextFile("dbfs:/temp/workshop/rdd/scala/ip_count")
# MAGIC   ipStatsCountAggregate.saveAsTextFile("dbfs:/temp/workshop/rdd/scala/ip_status_count")
# MAGIC }
# MAGIC catch {
# MAGIC   case _ : Throwable => println("Files already exist, deleting then re-saving")
# MAGIC }
# MAGIC finally {
# MAGIC   val srcPath: String= "dbfs:/temp/workshop/rdd/scala/ip_count"
# MAGIC   dbutils.fs.rm(srcPath, true)
# MAGIC   val srcPath2: String= "dbfs:/temp/workshop/rdd/scala/ip_status_count"
# MAGIC   dbutils.fs.rm(srcPath2, true)
# MAGIC
# MAGIC   // Re-save files
# MAGIC   ipCountAggregate.saveAsTextFile("dbfs:/temp/workshop/rdd/scala/ip_count")
# MAGIC   ipStatsCountAggregate.saveAsTextFile("dbfs:/temp/workshop/rdd/scala/ip_status_count")
# MAGIC }

# COMMAND ----------

# DBTITLE 1,Scala Standard Dataframe Functions
# MAGIC %scala 
# MAGIC import org.apache.spark.sql.types._;
# MAGIC import org.apache.spark.sql.functions._;
# MAGIC
# MAGIC
# MAGIC val df_1 = spark.read.option("sep", " ").option("quote", "\"").csv("dbfs:/databricks-datasets/sample_logs/");
# MAGIC
# MAGIC val df_bigger = df_1.union(df_1).union(df_1).union(df_1).union(df_1).union(df_1).union(df_1).union(df_1).union(df_1).union(df_1);
# MAGIC
# MAGIC val df_raw = df_bigger.toDF("ip", "mac", "user_id", "timestamp_raw", "timezone_raw", "api_call", "status", "code");
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
# MAGIC println("Aggregates for API calls by IP: ")
# MAGIC df_agg_by_ip.collect().foreach(println)
# MAGIC
# MAGIC // Already simpler and more robust
# MAGIC df_agg_by_ip.write.mode("overwrite").csv("dbfs:/temp/workshop/df/scala/ip_count")
# MAGIC df_agg_by_ip_status.write.mode("overwrite").csv("dbfs:/temp/workshop/df/scala/ip_status_count")

# COMMAND ----------

# DBTITLE 1,Python Standard Dataframe Functions
from pyspark.sql.functions import *

df_1 = spark.read.option("sep", " ").option("quote", "\"").csv('dbfs:/databricks-datasets/sample_logs/')

df_bigger = df_1.union(df_1).union(df_1).union(df_1).union(df_1).union(df_1).union(df_1).union(df_1).union(df_1).union(df_1)

df_raw = df_bigger.toDF(*["ip", "mac", "user_id", "timestamp_raw", "timezone_raw", "api_call", "status", "code"])

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

print("Aggregates for API calls by IP: ")
print(df_agg_by_ip_python.collect())

df_agg_by_ip_python.write.mode("overwrite").csv("dbfs:/temp/workshop/df/python/ip_count")
df_agg_by_ip_status_python.write.mode("overwrite").csv("dbfs:/temp/workshop/df/python/ip_status_count")


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## What if I need some very custom row-level processing in a UDF? 
# MAGIC
# MAGIC 1. Scala UDF in Scala - Great, if you use Data Frames wherever you can!
# MAGIC 2. Python UDF in Python - Not great for high performance! Even with Dataframes
# MAGIC 3. Scala UDF in Python - Great! Best of both worlds

# COMMAND ----------

# DBTITLE 1,Scala UDF to perform custom transform on a Row
# MAGIC %scala 
# MAGIC import java.time.{LocalDateTime, ZoneOffset}
# MAGIC import java.time.format.DateTimeFormatter
# MAGIC
# MAGIC def cleanTimestamp(rawTs: String) : String =  {
# MAGIC
# MAGIC   val cleanTs : String = rawTs.replaceAll("\\[", "")
# MAGIC   val inputFormatter = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss")
# MAGIC   val outputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")
# MAGIC
# MAGIC   val localDateTime = LocalDateTime.parse(cleanTs, inputFormatter)
# MAGIC   val utcDateTime = localDateTime.atOffset(ZoneOffset.UTC)
# MAGIC
# MAGIC   outputFormatter.format(utcDateTime)
# MAGIC
# MAGIC }
# MAGIC
# MAGIC
# MAGIC spark.udf.register("cleanTimestamp", cleanTimestamp(_))

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

from datetime import datetime
from dateutil import parser
import pytz


@udf("string")
def clean_timestamp_python(raw_ts):
  
  input_format = "%d/%b/%Y:%H:%M:%S"
  output_format = "%Y-%m-%dT%H:%M:%S"
  clean_ts = str(raw_ts).replace("[", "")

  dt_object = datetime.strptime(clean_ts, input_format)

  input_timezone = pytz.timezone('UTC')
  dt_object = input_timezone.localize(dt_object)

  utc_datetime = dt_object.astimezone(pytz.utc)

  output_ts = utc_datetime.strftime(output_format)

  return str(output_ts)

# COMMAND ----------

df_cleaned.write.format("delta").mode("overwrite").saveAsTable("df_cleaned")

df_cleaned_table = spark.table("df_cleaned")

df_cleaned_table.createOrReplaceTempView("df_cleaned_table")

display(df_cleaned)

# COMMAND ----------

# MAGIC %python 
# MAGIC
# MAGIC df_clean_ts = spark.sql("""SELECT *, cleanTimestamp(timestamp_raw) AS clean_ts FROM df_cleaned_table""")
# MAGIC
# MAGIC ## Continue doing stuff in Python Data Frames
# MAGIC
# MAGIC display(df_clean_ts)

# COMMAND ----------

df_clean_ts_python_udf = (df_cleaned_table
                          .select("*")
                          .withColumn("clean_ts", clean_timestamp_python(col("timestamp_raw")))
)

## Continue doing stuff in Python Data Frames

display(df_clean_ts_python_udf)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Pivoting Spark Plans in Photon

# COMMAND ----------

# DBTITLE 1,What is happening here? Is Photon Helpful
from pyspark.sql.functions import first

## Generally Pretty expensive
display(df_clean_ts
 .withColumn("date", col("clean_ts").cast("date"))
    .groupby(df_clean_ts.ip, df_clean_ts.clean_ts)
    .pivot("date")
    .agg(first("user_id"))
    )

# COMMAND ----------

display(df_clean_ts)
