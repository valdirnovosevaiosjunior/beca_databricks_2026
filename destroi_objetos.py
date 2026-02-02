# Databricks notebook source
# MAGIC %run ./pre_requisito

# COMMAND ----------

# DBTITLE 1,Drop schema
print(f"Drop Schema {schema}")
spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")