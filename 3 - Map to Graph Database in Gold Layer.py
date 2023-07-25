# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Take optimized ingest tables and build features, run analytics on Graph data model
# MAGIC

# COMMAND ----------

spark.sql(f"USE cyberworkshop;")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC SELECT * FROM bronze_logs_search;
