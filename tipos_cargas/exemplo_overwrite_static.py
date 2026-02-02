# Databricks notebook source
# MAGIC %run ../pre_requisito
import csv
import random
import uuid
from datetime import datetime, timedelta
import os

# Configurações
NUM_ARQUIVOS = 100
LINHAS_POR_ARQUIVO = 100
PASTA_SAIDA = "dados_vendas"

# Produtos fictícios
PRODUTOS = [
    (1, "Notebook"),
    (2, "Mouse"),
    (3, "Teclado"),
    (4, "Monitor"),
    (5, "Headset"),
    (6, "Cadeira Gamer"),
    (7, "Webcam"),
    (8, "HD Externo"),
    (9, "SSD"),
    (10, "Smartphone"),
]

# Cria pasta de saída
os.makedirs(PASTA_SAIDA, exist_ok=True)

def gerar_timestamp():
    inicio = datetime.now() - timedelta(days=30)
    fim = datetime.now()
    return inicio + (fim - inicio) * random.random()

for i in range(1, NUM_ARQUIVOS + 1):
    nome_arquivo = f"{PASTA_SAIDA}/vendas_{i:03d}.csv"

    with open(nome_arquivo, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow([
            "codigo_venda",
            "numero_fiscal",
            "id_produto",
            "nome_produto",
            "valor",
            "timestamp_venda"
        ])

        for _ in range(LINHAS_POR_ARQUIVO):
            id_produto, nome_produto = random.choice(PRODUTOS)

            writer.writerow([
                str(uuid.uuid4()),
                random.randint(100000000, 999999999),
                id_produto,
                nome_produto,
                round(random.uniform(10.0, 5000.0), 2),
                gerar_timestamp().isoformat()
            ])

print("🚀 Massa de dados gerada com sucesso!")
