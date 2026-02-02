# Databricks notebook source
# MAGIC %run ./pre_requisito

# COMMAND ----------

# DBTITLE 1,Drop schema
print(f"Drop Schema {schema}")
spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")

# COMMAND ----------
print(f"Drop Volume {volume}")
spark.sql(f"DROP VOLUME IF EXISTS {volume}")