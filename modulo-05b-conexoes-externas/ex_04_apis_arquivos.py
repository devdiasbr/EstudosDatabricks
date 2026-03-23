# Databricks notebook source

# MAGIC %md
# MAGIC # Exercício 4 — Ingestão de APIs REST e Arquivos em Landing Zones
# MAGIC
# MAGIC **Módulo 05-B — Conexões com Fontes Externas**
# MAGIC
# MAGIC **Objetivo:** Consumir dados de APIs REST, processar arquivos diversos (CSV, JSON, Parquet,
# MAGIC Avro, XML) em landing zones, e entender formatos do exame.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 — Consumindo APIs REST no Databricks

# COMMAND ----------

# --- API REST com requests ---
# %pip install requests

import json

# Exemplo: API pública (JSONPlaceholder)
# import requests
#
# response = requests.get("https://jsonplaceholder.typicode.com/posts")
# data = response.json()
#
# df_api = spark.createDataFrame(data)
# df_api.display()

print("API REST — leitura com requests")

# COMMAND ----------

# --- API REST com paginação ---

# def fetch_all_pages(base_url, max_pages=10):
#     """Busca dados paginados de uma API REST"""
#     all_data = []
#     for page in range(1, max_pages + 1):
#         response = requests.get(f"{base_url}?page={page}&per_page=100")
#         data = response.json()
#         if not data:
#             break
#         all_data.extend(data)
#     return all_data
#
# # Exemplo
# dados = fetch_all_pages("https://api.exemplo.com/v1/vendas")
# df = spark.createDataFrame(dados)

print("API REST — leitura com paginação")

# COMMAND ----------

# --- API REST com autenticação ---

# # Bearer Token
# headers = {
#     "Authorization": f"Bearer {dbutils.secrets.get(scope='api', key='token')}",
#     "Content-Type": "application/json"
# }
# response = requests.get("https://api.exemplo.com/v1/dados", headers=headers)
#
# # API Key
# params = {
#     "api_key": dbutils.secrets.get(scope="api", key="api-key"),
#     "format": "json"
# }
# response = requests.get("https://api.exemplo.com/v1/dados", params=params)
#
# # OAuth2
# from requests.auth import HTTPBasicAuth
# token_response = requests.post(
#     "https://auth.exemplo.com/oauth/token",
#     auth=HTTPBasicAuth(client_id, client_secret),
#     data={"grant_type": "client_credentials"}
# )
# access_token = token_response.json()["access_token"]

print("API REST — autenticação: Bearer, API Key, OAuth2")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Padrão Recomendado: API → Landing → Delta
# MAGIC
# MAGIC ```python
# MAGIC # 1. Buscar dados da API
# MAGIC data = fetch_all_pages("https://api.exemplo.com/v1/vendas")
# MAGIC
# MAGIC # 2. Salvar como JSON na landing zone
# MAGIC df = spark.createDataFrame(data)
# MAGIC df.write.mode("overwrite").json("/landing/api/vendas/")
# MAGIC
# MAGIC # 3. Auto Loader pega da landing → bronze
# MAGIC # (configurado no pipeline DLT ou workflow)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4.2 — Leitura de Arquivos: Todos os Formatos

# COMMAND ----------

# MAGIC %md
# MAGIC ### CSV

# COMMAND ----------

# --- CSV ---
# df_csv = spark.read \
#     .format("csv") \
#     .option("header", "true") \
#     .option("inferSchema", "true") \
#     .option("delimiter", ";") \
#     .option("encoding", "UTF-8") \
#     .option("quote", '"') \
#     .option("escape", '"') \
#     .option("multiLine", "true") \
#     .load("/landing/vendas/*.csv")

# Com schema explícito (recomendado em produção)
# from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType
# schema = StructType([
#     StructField("id", StringType()),
#     StructField("produto", StringType()),
#     StructField("valor", DoubleType()),
#     StructField("data", DateType())
# ])
# df_csv = spark.read.csv("/landing/vendas/", header=True, schema=schema)

print("CSV — com opções de parsing e schema explícito")

# COMMAND ----------

# MAGIC %md
# MAGIC ### JSON

# COMMAND ----------

# --- JSON ---
# Linha por linha (JSON Lines) — mais comum em data engineering
# df_json = spark.read.json("/landing/api/vendas/")

# JSON multilinha (um JSON completo por arquivo)
# df_json = spark.read \
#     .option("multiLine", "true") \
#     .json("/landing/api/vendas/")

# JSON aninhado → flatten
# df_flat = df_json.select(
#     col("id"),
#     col("dados.nome").alias("nome"),
#     explode(col("dados.itens")).alias("item")
# )

print("JSON — single line, multiline, e flatten")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parquet e Delta

# COMMAND ----------

# --- Parquet ---
# Formato colunar, compressão nativa, schema embutido
# df_parquet = spark.read.parquet("/landing/dados/vendas/")

# --- Delta ---
# Parquet + Transaction Log
# df_delta = spark.read.format("delta").load("/delta/vendas/")

# --- Via SQL ---
# SELECT * FROM parquet.`/landing/dados/vendas/`
# SELECT * FROM delta.`/delta/vendas/`

print("Parquet e Delta — formatos otimizados")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Avro

# COMMAND ----------

# --- Avro ---
# Formato binário, schema embutido, popular em Kafka/streaming
# df_avro = spark.read.format("avro").load("/landing/kafka/eventos/")

# Avro com schema registry (Kafka)
# df_avro = spark.read \
#     .format("avro") \
#     .option("avroSchema", schema_json) \
#     .load("/landing/kafka/eventos/")

print("Avro — formato binário com schema embutido")

# COMMAND ----------

# MAGIC %md
# MAGIC ### XML

# COMMAND ----------

# --- XML ---
# %pip install spark-xml
# df_xml = spark.read \
#     .format("xml") \
#     .option("rowTag", "registro") \
#     .load("/landing/xml/notas_fiscais/")

# Exemplo de XML:
# <notas>
#   <registro>
#     <id>1</id>
#     <valor>150.00</valor>
#     <cliente>Maria</cliente>
#   </registro>
# </notas>

print("XML — leitura com rowTag")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Resumo de Formatos
# MAGIC
# MAGIC | Formato | Tipo | Schema | Compressão | Uso Típico |
# MAGIC |---------|------|--------|------------|------------|
# MAGIC | **CSV** | Texto | ❌ Externo | ❌ Não | Exports, legado |
# MAGIC | **JSON** | Texto | ❌ Externo | ❌ Não | APIs, logs |
# MAGIC | **Parquet** | Binário colunar | ✅ Embutido | ✅ Snappy/Gzip | Analytics, DW |
# MAGIC | **Delta** | Parquet + Log | ✅ Embutido | ✅ Snappy/Gzip | Lakehouse (padrão) |
# MAGIC | **Avro** | Binário row-based | ✅ Embutido | ✅ Deflate | Kafka, streaming |
# MAGIC | **XML** | Texto | ❌ Externo | ❌ Não | Sistemas legado, NF-e |
# MAGIC | **ORC** | Binário colunar | ✅ Embutido | ✅ Zlib | Hive (legado) |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercícios

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício A — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Qual formato armazena o schema embutido E é otimizado para leitura colunar?
# MAGIC
# MAGIC - a) CSV
# MAGIC - b) JSON
# MAGIC - c) Parquet
# MAGIC - d) Avro
# MAGIC
# MAGIC > _Sua resposta: ___
# MAGIC >
# MAGIC > **Gabarito:** c) Parquet é colunar com schema embutido. Avro também tem schema, mas é row-based.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício B — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Ao ler um CSV com delimitador `;` e encoding `ISO-8859-1`, qual configuração está correta?
# MAGIC
# MAGIC - a) `spark.read.csv(path, sep=";", encoding="ISO-8859-1")`
# MAGIC - b) `spark.read.format("csv").option("delimiter", ";").option("encoding", "ISO-8859-1").load(path)`
# MAGIC - c) `spark.read.format("csv").option("sep", ";").option("charset", "ISO-8859-1").load(path)`
# MAGIC - d) Ambas b) e c) estão corretas
# MAGIC
# MAGIC > _Sua resposta: ___
# MAGIC >
# MAGIC > **Gabarito:** b) `delimiter` e `encoding` são as opções padrão do Spark CSV reader.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício C — Prático: API → Landing → Delta
# MAGIC
# MAGIC Complete o pipeline:
# MAGIC
# MAGIC ```python
# MAGIC import requests
# MAGIC
# MAGIC # 1. Buscar dados da API
# MAGIC response = requests.get("https://jsonplaceholder.typicode.com/users")
# MAGIC data = response.____()  # converter para Python list
# MAGIC
# MAGIC # 2. Criar DataFrame
# MAGIC df = spark.____(____)
# MAGIC
# MAGIC # 3. Salvar na landing zone como JSON
# MAGIC df.write.mode("____").____.("____")
# MAGIC
# MAGIC # 4. Ler da landing e salvar como Delta (bronze)
# MAGIC df_bronze = spark.read.____("____")
# MAGIC df_bronze.write.format("____").mode("____").saveAsTable("bronze.usuarios_api")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício D — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Qual é a melhor prática para armazenar credenciais de APIs no Databricks?
# MAGIC
# MAGIC - a) Hardcoded no notebook
# MAGIC - b) Variáveis de ambiente do cluster
# MAGIC - c) Databricks Secrets com scope vinculado ao Key Vault
# MAGIC - d) Arquivo `.env` no workspace
# MAGIC
# MAGIC > _Sua resposta: ___
# MAGIC >
# MAGIC > **Gabarito:** c) Secrets integrados ao Key Vault/Secret Manager é a prática recomendada.
