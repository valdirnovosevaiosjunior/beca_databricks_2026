# Databricks notebook source
# MAGIC %md
# MAGIC ## Exercício: Evolução de Esquema (Schema Evolution)
# MAGIC 
# MAGIC Neste exercício, você irá praticar a evolução de esquema em tabelas Delta.
# MAGIC 
# MAGIC 1. No arquivo `01_exemplo_overwrite_static.py` é gerada a tabela `beca_2026_{extra_name}.viagens_taxi`.
# MAGIC 2. Sua tarefa é incluir uma nova coluna calculada chamada `tempo_de_viagem`, que deve conter o tempo de viagem no formato `00h:00m:00s`.
# MAGIC 3. Implemente duas variações de carga:
# MAGIC    - **Carga Full (Overwrite):** Reescreva a tabela inteira incluindo a nova coluna.
# MAGIC    - **Carga Incremental (Merge):** Utilize o comando `merge` para atualizar apenas a nova coluna, sem regravar todas as colunas da tabela.
# MAGIC 
# MAGIC O objetivo é comparar as abordagens de evolução de esquema e entender os impactos de cada estratégia.