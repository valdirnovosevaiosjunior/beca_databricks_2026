# Databricks notebook source
# MAGIC %run ../pre_requisito

# COMMAND ----------

# DBTITLE 1,drop se existir
spark.sql(f"DROP TABLE IF EXISTS {schema}.bolos_vendidos_dia")

# COMMAND ----------

# DBTITLE 1,Primeira Carga
from pyspark.sql import functions as F

df = (
    spark.table("samples.bakehouse.sales_transactions")
         .withColumn(
             "transaction_date",
             F.to_date("dateTime")
         )
         .filter("DATE(dateTime) = '2024-05-01'")
         .withColumn("created_date", F.current_timestamp())
)

df.printSchema()

(
    df.write
      .format("delta")
      .mode("overwrite")
      .partitionBy("transaction_date")
    .saveAsTable(f"{schema}.bolos_vendidos_dia")
)

# COMMAND ----------

# DBTITLE 1,Consulta
display(spark.sql(f"""
    select transaction_date, 
    count(*), 
    max(created_date) 
    from {schema}.bolos_vendidos_dia
    group by all
    order by 1 desc
"""))

# COMMAND ----------

# DBTITLE 1,Segunda Carga
from delta.tables import DeltaTable
from pyspark.sql import functions as F

df_source = (
    spark.table("samples.bakehouse.sales_transactions")
         .withColumn("transaction_date", F.to_date("dateTime"))
         .withColumn("created_date", F.current_timestamp())
)

target_table = f"{schema}.bolos_vendidos_dia"

delta_target = DeltaTable.forName(
    spark,
    target_table
)

(
    delta_target.alias("t")
        .merge(
            df_source.alias("s"),
            "t.transactionID = s.transactionID"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
).show()

# COMMAND ----------

# DBTITLE 1,Consulta
display(
spark.sql(f"""
    select transaction_date, 
    count(*), 
    max(created_date) 
    from {schema}.bolos_vendidos_dia
    group by all
    order by 1 desc
"""))

# COMMAND ----------

# DBTITLE 1,Histórico de Versões
display(
    spark.sql(f"DESCRIBE HISTORY {schema}.bolos_vendidos_dia")
)

# COMMAND ----------

# DBTITLE 1,Show create table
display(
    spark.sql(f"SHOW CREATE TABLE {schema}.bolos_vendidos_dia")
)