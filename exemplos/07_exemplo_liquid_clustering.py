# Databricks notebook source
# MAGIC %run ../pre_requisito

# COMMAND ----------

# DBTITLE 1,drop se existir
spark.sql(f"DROP TABLE IF EXISTS {schema}.viagens_liquid_clustering")

# COMMAND ----------

# DBTITLE 1,Primeira Carga
from pyspark.sql import functions as F

df = (
    spark.table("samples.nyctaxi.trips")
         .withColumn(
             "pickup_date",
             F.to_date("tpep_pickup_datetime")
         )
         .filter("tpep_pickup_datetime >= '2016-01-01 00:00:00' AND tpep_pickup_datetime < '2016-02-01 00:00:00'")
         .withColumn("created_date", F.current_timestamp())
)

df.printSchema()

(
    df.write
      .format("delta")
      .mode("overwrite")
      .clusterBy("pickup_date", "pickup_zip")
    .saveAsTable(f"{schema}.viagens_liquid_clustering")
)

# COMMAND ----------

# DBTITLE 1,Consulta 
display(spark.sql(f"""
    select * from {schema}.viagens_liquid_clustering
"""))

# COMMAND ----------

# DBTITLE 1,Show create table de origem
display(spark.sql(f"show create table {schema}.viagens_liquid_clustering"))

# COMMAND ----------

# DBTITLE 1,Histórico de Versões
display(spark.sql(f"describe history {schema}.viagens_liquid_clustering"))

# COMMAND ----------

# DBTITLE 1,Describe Detail
display(spark.sql(f"describe detail {schema}.viagens_liquid_clustering"))