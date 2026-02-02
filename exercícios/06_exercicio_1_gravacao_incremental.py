# Databricks notebook source

# MAGIC %md
# MAGIC ## Desafio: Gravação Incremental de Reservas - WanderBricks
# MAGIC 
# MAGIC A WanderBricks, empresa do setor de turismo, precisa consolidar as informações de reservas a partir de duas fontes:
# MAGIC 
# MAGIC 1. **Tabela base:** `samples.wanderbricks.bookings` (contém o histórico completo de reservas)
# MAGIC 2. **Tabela incremental:** `samples.wanderbricks.booking_updates` (contém atualizações recentes de reservas)
# MAGIC 
# MAGIC ### Objetivo
# MAGIC Criar uma tabela **silver** unificada, contendo todos os dados da tabela base e as atualizações mais recentes da tabela incremental, sem duplicidade de chaves.
# MAGIC 
# MAGIC ### Regras e Observações
# MAGIC - As tabelas possuem granularidades diferentes: a incremental pode conter múltiplas atualizações para o mesmo `booking_id`.
# MAGIC - Para cada `booking_id`, apenas a atualização mais recente (com base em `updated_at`) deve ser considerada.
# MAGIC - Antes de realizar a junção, analise as tabelas para identificar corretamente as chaves e entender a estrutura dos dados.
# MAGIC - As colunas `check_in`, `check_out`, `guests_count`, `total_amount`, `status` e `updated_at` devem ser atualizadas conforme os dados mais recentes da incremental.
# MAGIC - A tabela final **não pode conter chaves duplicadas**.
# MAGIC 
# MAGIC > **Dica:** Utilize operações de window function ou agregação para garantir que apenas o registro mais recente de cada reserva seja considerado.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from samples.wanderbricks.bookings

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from samples.wanderbricks.booking_updates