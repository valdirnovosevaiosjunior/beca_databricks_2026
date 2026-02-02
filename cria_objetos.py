# Databricks notebook source
# MAGIC %run ./pre_requisito

# COMMAND ----------

# DBTITLE 1,Variáveis Globais será utilizado nos notebooks
print(extra_name)
print(schema)
print(volume)

# COMMAND ----------

# DBTITLE 1,create schema
print(f"Criando Schema {schema}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

# COMMAND ----------
print(f"Criando Volume {volume}")
spark.sql(f"CREATE VOLUME {volume}")