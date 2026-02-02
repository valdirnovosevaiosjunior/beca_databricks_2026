# Databricks notebook source
# MAGIC %run ../pre_requisito

# COMMAND ----------

# DBTITLE 1,drop se existir
spark.sql(f"DROP TABLE IF EXISTS {schema}.viagens_taxi_dia")

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
      .partitionBy("pickup_date")
      .option("partitionOverwriteMode", "dynamic")
    .saveAsTable(f"{schema}.viagens_taxi_dia")
)

# COMMAND ----------

# DBTITLE 1,Consulta
display(spark.sql(f"""
    select pickup_date, 
    count(*), 
    max(created_date) 
    from {schema}.viagens_taxi_dia 
    group by all
    order by 1 desc
"""))

# COMMAND ----------

# DBTITLE 1,Segunda Carga
from pyspark.sql import functions as F

df = (
    spark.table("samples.nyctaxi.trips")
         .withColumn(
             "pickup_date",
             F.to_date("tpep_pickup_datetime")
         )
         .filter("tpep_pickup_datetime >= '2016-01-15 00:00:00'")
         .withColumn("created_date", F.current_timestamp())
)

df.printSchema()

(
    df.write
      .format("delta")
      .mode("overwrite")
      .partitionBy("pickup_date")
      .option("partitionOverwriteMode", "dynamic")
    .saveAsTable(f"{schema}.viagens_taxi_dia")
)

# COMMAND ----------

# DBTITLE 1,Consulta
display(spark.sql(f"""
    select pickup_date, 
    count(*), 
    max(created_date) 
    from {schema}.viagens_taxi_dia 
    group by all
    order by 1 
"""))

# COMMAND ----------

# DBTITLE 1,Show create table
display(spark.sql(f"show create table {schema}.viagens_taxi_dia"))

# COMMAND ----------

display(spark.sql(f"describe history {schema}.viagens_taxi_dia"))