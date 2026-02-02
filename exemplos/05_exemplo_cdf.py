# Databricks notebook source
# MAGIC %run ../pre_requisito

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DateType

SOURCE_TABLE = f"{schema}.bolos_vendidos_dia"
TARGET_TABLE = f"{schema}.bolos_vendidos_mes"
CHECKPOINT_PATH = f"/Volumes/workspace/{schema}/checks_points/"

# COMMAND ----------

# DBTITLE 1,drop se existir
spark.sql(f"DROP TABLE IF EXISTS {TARGET_TABLE}")

# COMMAND ----------

# DBTITLE 1,remover checkpoint da tabela final
dbutils.fs.rm(f"{CHECKPOINT_PATH}", True)

# COMMAND ----------

# DBTITLE 1,Ativa CDF na tabela de origem
spark.sql(f"ALTER TABLE {schema}.bolos_vendidos_dia SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

# COMMAND ----------

# DBTITLE 1,Show create table de origem
display(
    spark.sql(f"SHOW CREATE TABLE beca_2026_{extra_name}.bolos_vendidos_dia")
)    

# COMMAND ----------

# DBTITLE 1,método agregação
def aggregate_monthly(df):
    return (
        df
        .withColumn(
            "transaction_month",
            F.date_trunc("month", F.col("transaction_date"))
        )
        .groupBy(
            "customerID",
            "franchiseID",
            "product",
            "transaction_month"
        )
        .agg(
            F.sum("quantity").alias("total_quantity"),
            F.sum("totalPrice").cast("decimal(10,2)").alias("total_price")
        )
    )

# COMMAND ----------

# DBTITLE 1,Primeira Carga
df_initial = spark.table(SOURCE_TABLE)

df_monthly_initial = aggregate_monthly(df_initial)

(
    df_monthly_initial
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

# COMMAND ----------

# DBTITLE 1,Query agregada
display(
    spark.sql(f"select * from beca_2026_{extra_name}.bolos_vendidos_mes")
)               

# COMMAND ----------

# DBTITLE 1,Insert dados na origem
from pyspark.sql.types import *
from datetime import datetime, date

data = [
    (
        1003006,  # transactionID
        2000004,  # customerID
        3000047,  # franchiseID
        datetime(2024, 5, 14, 10, 12, 30),  # dateTime (TIMESTAMP)
        "Austin Almond Biscotti",
        10,  # quantity
        3,   # unitPrice
        30,  # totalPrice
        "visa",
        4111111111111111,  # cardNumber (BIGINT)
        date(2024, 5, 14)  # transaction_date (DATE)
    ),
    (
        1003007,
        2000004,
        3000047,
        datetime(2024, 5, 20, 18, 45, 10),
        "Austin Almond Biscotti",
        6,
        3,
        18,
        "mastercard",
        5555555555554444,
        date(2024, 5, 20)
    ),
    (
        1003008,
        2000172,
        3000047,
        datetime(2024, 6, 2, 9, 5, 55),
        "Chocolate Lava Cake",
        4,
        12,
        48,
        "amex",
        378282246310005,
        date(2024, 6, 2)
    ),
    (
        1003009,
        2000259,
        3000046,
        datetime(2024, 6, 18, 14, 33, 21),
        "Chocolate Lava Cake",
        2,
        12,
        24,
        "visa",
        4012888888881881,
        date(2024, 6, 18)
    ),
    (
        1003010,
        2000259,
        3000046,
        datetime(2024, 7, 1, 11, 22, 40),
        "Lemon Pound Cake",
        5,
        8,
        40,
        "mastercard",
        5105105105105100,
        date(2024, 7, 1)
    )
]


schema = StructType([
    StructField("transactionID", LongType(), False),
    StructField("customerID", LongType(), False),
    StructField("franchiseID", LongType(), False),
    StructField("dateTime", TimestampType(), True),
    StructField("product", StringType(), True),
    StructField("quantity", LongType(), True),
    StructField("unitPrice", LongType(), True),
    StructField("totalPrice", LongType(), True),
    StructField("paymentMethod", StringType(), True),
    StructField("cardNumber", LongType(), True),
    StructField("transaction_date", DateType(), True),
])

df_insert = spark.createDataFrame(data, schema)

df_insert = df_insert.withColumn(
    "created_date",
    F.current_timestamp()
)

(
    df_insert
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(f"beca_2026_{extra_name}.bolos_vendidos_dia")
)

# COMMAND ----------

# DBTITLE 1,Conferindo o CDF na tabela de origem
history_df = spark.sql(f"DESCRIBE HISTORY {SOURCE_TABLE}")

max_version = (
    history_df
    .agg(F.max("version").alias("max_version"))
    .collect()[0]["max_version"]
)

print(f"Última versão da tabela: {max_version}")

starting_version = max(0, max_version - 1)

print(f"Lendo CDF a partir da versão: {starting_version}")

df_leitura = (
    spark.read
    .format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", starting_version)
    .table(SOURCE_TABLE)
)

display(df_leitura)

# COMMAND ----------

# DBTITLE 1,Carga incremental usando CDF
df_cdf = (
    spark.readStream
    .format("delta")
    .option("readChangeFeed", "true")
    .table(SOURCE_TABLE)
)

df_cdf_filtered = (
    df_cdf
    .filter(F.col("_change_type").isin("insert", "update_postimage"))
)

df_monthly_stream = aggregate_monthly(df_cdf_filtered)


def merge_monthly_aggregation(microbatch_df, batch_id):

    microbatch_df.createOrReplaceTempView("monthly_updates")

    spark.sql(f"""
        MERGE INTO {TARGET_TABLE} AS target
        USING monthly_updates AS source
        ON
            target.customerID = source.customerID AND
            target.franchiseID = source.franchiseID AND
            target.product = source.product AND
            target.transaction_month = source.transaction_month
        WHEN MATCHED THEN UPDATE SET
            target.total_quantity = source.total_quantity,
            target.total_price = source.total_price
        WHEN NOT MATCHED THEN INSERT *
    """)
print(CHECKPOINT_PATH)

(
    df_monthly_stream
    .writeStream
    .foreachBatch(merge_monthly_aggregation)
    .outputMode("update")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(once=True)
    .start()
)


# COMMAND ----------

# DBTITLE 1,Consulta 1
display(
    spark.sql(f"""select * from beca_2026_{extra_name}.bolos_vendidos_mes""")
)

# COMMAND ----------

# DBTITLE 1,Consulta 2
display(
    spark.sql(f"""select * from beca_2026_{extra_name}.bolos_vendidos_mes 
              where transaction_month = '2024-07-01' and product LIKE 'Lemon%'
    """)
)

# COMMAND ----------

# DBTITLE 1,Histórico de Versões
display(
    spark.sql(f"DESCRIBE HISTORY beca_2026_{extra_name}.bolos_vendidos_mes")
)