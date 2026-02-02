# Databricks notebook source
# MAGIC %run ../pre_requisito

# COMMAND ----------
def process_all_tables():    
    # Busca todas as tabelas do schema
    tables_df = spark.sql(f"SELECT table_name FROM system.information_schema.tables WHERE table_schema = '{schema}'")
    for table_row in tables_df.collect():
        table = table_row['table_name']
        full_table = f"{schema}.{table}"
        print(f"Processando tabela: {full_table}")
        # VACUUM
        spark.sql(f"VACUUM {full_table}")
        # OPTIMIZE
        spark.sql(f"OPTIMIZE {full_table}")
        # ANALYZE
        spark.sql(f"ANALYZE TABLE {full_table} COMPUTE STATISTICS FOR ALL COLUMNS")

if __name__ == "__main__":
    process_all_tables()
    print("Processamento concluído.")
