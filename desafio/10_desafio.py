
# Databricks notebook source
# MAGIC %md
# Desafio: Pipeline de Dados de Vendas

## Objetivo
Construir um pipeline de ingestão e processamento de dados de vendas, utilizando boas práticas de arquitetura em camadas (Bronze, Silver e Gold) e métodos eficientes de carga incremental.

## Instruções
1. **Ingestão dos Dados (Bronze):**
   - Ler os 100 arquivos de vendas disponíveis.
   - Utilize um dos métodos de carga disponíveis (ex: leitura direta, AutoLoader, etc.).
   - Adicione colunas extras: data de carga e nome do arquivo.
   - Garanta que a carga seja incremental, processando apenas arquivos novos (evite full load).

2. **Transformação (Silver):**
   - Utilize o método de carga MERGE para atualizar/inserir dados na camada Silver.
   - Escolha livremente o tipo de tabela (particionada ou com liquid clustering).
   - Mantenha a lógica incremental, processando apenas dados novos.

3. **Modelagem Analítica (Gold):**
   - Crie duas tabelas:
     - **Fato de Vendas:** tabela detalhada com todos os registros de vendas.
     - **Tabela Agregada:** tabela com o somatório da(s) medida(s) de interesse (ex: valor total de vendas por período, produto, etc.).
   - Assegure que ambas as tabelas sejam alimentadas de forma incremental.

## Requisitos Gerais
- Todos os processos devem evitar cargas completas (full load), processando apenas dados novos a cada execução.
- Documente as decisões tomadas e os métodos utilizados.
- Utilize boas práticas de organização de código e versionamento.
- Fazer a orquestração das cargas das camadas com Databricks Workflows.
- Salvar os notebooks em um repositório Git conectado ao Databricks.
- Utilizar variáveis de ambiente para gerenciar caminhos e configurações.
- Implementar monitoramento básico para acompanhar o status das cargas (ex: logs simples).
- Garantir que o pipeline seja idempotente, ou seja, múltiplas execuções com os mesmos dados não devem causar duplicações ou inconsistências.
- Incluir tratamento de erros para capturar e registrar falhas durante o processo de ingestão e transformação.
- Validar os dados em cada etapa para assegurar a qualidade (ex: checar por valores nulos, formatos corretos, etc.).
- Utilizar notebooks separados para cada camada (Bronze, Silver, Gold) para melhor organização e manutenção do código.
- Documentar o código com comentários explicativos e utilizar markdown para descrever cada etapa do processo.
- Usar Spark com Formato Delta e sem uso de pandas.