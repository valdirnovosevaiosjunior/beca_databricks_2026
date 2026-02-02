# Databricks notebook source
# MAGIC %run ./pre_requisito

# COMMAND ----------

# DBTITLE 1,create schema
print(f"Criando Schema {schema}")
spark.sql(f"DROP SCHEMA IF NOT EXISTS {schema} CASCADE")

# COMMAND ----------
print(f"Criando Volume {volume}")
spark.sql(f"DROP VOLUME IF EXISTS {volume}")