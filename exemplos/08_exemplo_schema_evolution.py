# Databricks notebook source
# MAGIC %run ../pre_requisito

# COMMAND ----------

# DBTITLE 1,drop se existir
spark.sql(f"DROP TABLE IF EXISTS {schema}.tabela_evoluida")

# COMMAND ----------

# DBTITLE 1,Primeira Carga
df = spark.createDataFrame([("Joao", 47), ("Maria", 23), ("Leonardo", 51)]).toDF(
    "primeiro_nome", "idade"
)

(
    df
    .write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable(f"{schema}.tabela_evoluida")
)

# COMMAND ----------

# DBTITLE 1,Segunda Carga 
df = spark.createDataFrame([("Francisco", 68, "usa"), ("Juliana", 26, "brasil")]).toDF(
    "primeiro_nome", "idade", "pais"
)

(
    df
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(f"{schema}.tabela_evoluida")
)

# COMMAND ----------

# DBTITLE 1,Segunda Carga com Merge Schema
df = spark.createDataFrame([("Francisco", 68, "usa"), ("Juliana", 26, "brasil")]).toDF(
    "primeiro_nome", "idade", "pais"
)

(
    df
    .write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(f"{schema}.tabela_evoluida")
)

# COMMAND ----------

# DBTITLE 1,Consulta 
display(spark.sql(f"""
    select * from {schema}.tabela_evoluida
"""))

# COMMAND ----------

# DBTITLE 1,Histórico de Versões
display(spark.sql(f"describe history {schema}.tabela_evoluida"))