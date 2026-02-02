# 🚀 Desafio: Pipeline de Dados de Vendas

![Databricks](https://img.shields.io/badge/Databricks-Data%20Engineering-orange?logo=databricks)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-Incremental%20Load-blue?logo=delta)
![Spark](https://img.shields.io/badge/Spark-PySpark-red?logo=apache-spark)
![Status](https://img.shields.io/badge/Status-Em%20Desenvolvimento-yellow)

## 📋 Descrição

Este repositório contém a solução para o desafio de construção de um pipeline de ingestão e processamento incremental de dados de vendas, utilizando arquitetura em camadas (Bronze, Silver, Gold) com Databricks, Delta Lake e PySpark.

---

## 🏗️ Estrutura do Projeto

```
├── cria_objetos.py           # Criação de schema e volumes Delta
├── destroi_objetos.py        # Remoção de schema e volumes
├── pre_requisito.py          # Variáveis globais e configurações
├── desafio/
│   ├── 10_desafio.py         # Notebook principal do desafio
│   └── dados_vendas/        # Arquivos CSV de vendas (100 arquivos)
├── exemplos/                # Exemplos de cargas e operações Delta
├── exercícios/              # Exercícios práticos
├── tipos_cargas/            # Scripts de geração de massa de dados
└── README.md                # Este arquivo
```

---

## 🎯 Objetivo

Construir um pipeline de dados de vendas, seguindo boas práticas de arquitetura em camadas e métodos eficientes de carga incremental, com as seguintes etapas:

- **Bronze:** Ingestão incremental dos arquivos de vendas, adicionando colunas de data de carga e nome do arquivo.
- **Silver:** Transformação incremental usando MERGE, com atualização/inserção eficiente.
- **Gold:** Modelagem analítica com tabelas fato e agregada, ambas alimentadas incrementalmente.

---

## 🛠️ Tecnologias Utilizadas

- ![Python](https://img.shields.io/badge/Python-3.9+-blue?logo=python)
- ![PySpark](https://img.shields.io/badge/PySpark-Data%20Processing-orange?logo=apache-spark)
- ![Delta Lake](https://img.shields.io/badge/Delta%20Lake-Storage-blue?logo=delta)
- ![Databricks](https://img.shields.io/badge/Databricks-Workspace-orange?logo=databricks)
- ![Git](https://img.shields.io/badge/Git-Versionamento-critical?logo=git)

---

## 🧩 Camadas do Pipeline

### 🥉 Bronze
- Ingestão incremental dos arquivos CSV.
- Adição de colunas: `data_carga`, `nome_arquivo`.
- Processamento apenas de arquivos novos (idempotência).

### 🥈 Silver
- Transformação e limpeza dos dados.
- Carga incremental via MERGE.
- Tabelas particionadas ou com liquid clustering.

### 🥇 Gold
- Modelagem analítica:
	- **Fato de Vendas:** Detalhamento completo.
	- **Tabela Agregada:** Exemplo: total de vendas por período/produto.
- Atualização incremental.

---

## ⚙️ Boas Práticas e Requisitos

- 🚫 **Sem full load:** Apenas dados novos a cada execução.
- 📝 **Documentação e comentários** em todos os notebooks.
- 🗂️ **Notebooks separados** para cada camada (Bronze, Silver, Gold).
- 🔄 **Idempotência:** Múltiplas execuções não causam duplicidade.
- 🛡️ **Tratamento de erros** e logs simples para monitoramento.
- 🧪 **Validação de dados** em cada etapa (nulos, formatos, etc).
- 🔑 **Variáveis de ambiente** para caminhos e configs.
- 🗃️ **Versionamento** via Git e integração com Databricks Repos.
- 🧩 **Orquestração** com Databricks Workflows.

---

## 📚 Exemplos e Exercícios

- Exemplos práticos de:
	- Carga incremental (overwrite, dynamic, replaceWhere, merge)
	- Schema evolution
	- Liquid clustering
	- Change Data Feed (CDF)
- Exercícios para fixação dos conceitos.

---

## 🚦 Como Executar

1. Clone o repositório:
	 ```bash
	 git clone https://github.com/seu-usuario/seu-repo.git
	 ```
2. Importe os notebooks para o Databricks.
3. Execute `cria_objetos.py` para criar schema e volumes.
4. Siga a ordem: Bronze → Silver → Gold.
5. Utilize o Databricks Workflows para orquestração.

---