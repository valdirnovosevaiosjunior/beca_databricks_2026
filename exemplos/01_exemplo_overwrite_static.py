# Databricks notebook source
# MAGIC %run ../pre_requisito

# COMMAND ----------

spark.table(f"DROP TABLE IF EXISTS {schema}.viagens_taxi")

# COMMAND ----------

# DBTITLE 1,Carga 1
df = spark.table("samples.nyctaxi.trips")
(
    df.write
      .format("delta")
      .mode("overwrite")
      .partitionBy("pickup_zip")
      .saveAsTable(f"{schema}.viagens_taxi")
)

# COMMAND ----------

# DBTITLE 1,Select
display(spark.sql(f"select * from {schema}.viagens_taxi"))

# COMMAND ----------

# DBTITLE 1,Lista de Partições
display(spark.sql(f"show partitions {schema}.viagens_taxi"))

# COMMAND ----------

# DBTITLE 1,Show create table
display(spark.sql(f"show create table {schema}.viagens_taxi"))

# COMMAND ----------

# DBTITLE 1,Histórico de versões
display(spark.sql(f"describe history {schema}.viagens_taxi"))