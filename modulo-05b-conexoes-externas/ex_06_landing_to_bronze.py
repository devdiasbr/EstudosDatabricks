# Databricks notebook source

# MAGIC %md
# MAGIC # Exercício 6 — Pipeline Completo: Landing Zone → Bronze
# MAGIC
# MAGIC **Módulo 05-B — Conexões com Fontes Externas**
# MAGIC
# MAGIC **Objetivo:** Construir um pipeline end-to-end que ingere dados de múltiplas fontes
# MAGIC (cloud storage, JDBC, API) para a camada Bronze do Lakehouse.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Arquitetura do Pipeline
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────────────┐
# MAGIC │                         FONTES DE DADOS                                │
# MAGIC │                                                                         │
# MAGIC │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐               │
# MAGIC │  │ ADLS/S3  │  │SQL Server│  │  MongoDB  │  │ REST API │               │
# MAGIC │  │ (CSV)    │  │ (JDBC)   │  │ (Export)  │  │ (JSON)   │               │
# MAGIC │  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘               │
# MAGIC │       │              │              │              │                     │
# MAGIC └───────┼──────────────┼──────────────┼──────────────┼─────────────────────┘
# MAGIC         │              │              │              │
# MAGIC         ▼              ▼              ▼              ▼
# MAGIC ┌─────────────────────────────────────────────────────────────────────────┐
# MAGIC │                    LANDING ZONE (Cloud Storage)                         │
# MAGIC │  /landing/vendas/   /landing/jdbc/   /landing/mongo/  /landing/api/     │
# MAGIC └───────┬──────────────┬──────────────┬──────────────┬─────────────────────┘
# MAGIC         │              │              │              │
# MAGIC         ▼              ▼              ▼              ▼
# MAGIC ┌─────────────────────────────────────────────────────────────────────────┐
# MAGIC │                    AUTO LOADER (cloudFiles)                             │
# MAGIC │  Detecção automática de novos arquivos + Schema Evolution              │
# MAGIC └───────┬──────────────────────────────────────────────────────────────────┘
# MAGIC         │
# MAGIC         ▼
# MAGIC ┌─────────────────────────────────────────────────────────────────────────┐
# MAGIC │                    BRONZE (Delta Tables)                                │
# MAGIC │  bronze.vendas_csv  bronze.clientes_jdbc  bronze.pedidos_mongo  ...     │
# MAGIC │  + _metadata colunas: _rescued_data, _source_file, _ingest_timestamp   │
# MAGIC └─────────────────────────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 6.1 — Simulando as Fontes com Faker

# COMMAND ----------

# %pip install faker

# COMMAND ----------

from pyspark.sql.functions import (
    current_timestamp, input_file_name, lit, col
)

print("Imports carregados")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gerar dados CSV simulando export de ERP (ADLS/S3)

# COMMAND ----------

# from faker import Faker
# import csv
# import os
#
# fake = Faker("pt_BR")
# Faker.seed(42)
#
# # Simular 3 arquivos CSV (como se chegassem em dias diferentes)
# for dia in ["2024-01-15", "2024-01-16", "2024-01-17"]:
#     landing_path = f"/tmp/landing/vendas_csv/{dia}"
#     dbutils.fs.mkdirs(landing_path)
#
#     rows = []
#     for i in range(500):
#         rows.append({
#             "id": f"V-{dia}-{i:04d}",
#             "produto": fake.random_element(["Notebook", "Mouse", "Teclado", "Monitor", "Headset"]),
#             "valor": round(fake.pyfloat(min_value=50, max_value=5000, right_digits=2), 2),
#             "quantidade": fake.random_int(min=1, max=10),
#             "data_venda": dia,
#             "vendedor": fake.name(),
#             "cidade": fake.city()
#         })
#
#     df_csv = spark.createDataFrame(rows)
#     df_csv.write.mode("overwrite").option("header", "true").csv(f"{landing_path}/")
#
# print(f"3 batches de CSV gerados em /tmp/landing/vendas_csv/")

print("Gerador de CSV simulando landing zone (descomente para executar)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gerar dados JSON simulando export de API

# COMMAND ----------

# from faker import Faker
# import json
#
# fake = Faker("pt_BR")
#
# # Simular dados de API (JSON Lines)
# rows = []
# for i in range(1000):
#     rows.append({
#         "user_id": f"U-{i:05d}",
#         "evento": fake.random_element(["login", "compra", "logout", "visualizacao", "carrinho"]),
#         "pagina": fake.uri_path(),
#         "timestamp": fake.iso8601(),
#         "dispositivo": fake.random_element(["mobile", "desktop", "tablet"]),
#         "ip": fake.ipv4()
#     })
#
# df_json = spark.createDataFrame(rows)
# df_json.write.mode("overwrite").json("/tmp/landing/eventos_api/")
#
# print("Dados JSON de API gerados em /tmp/landing/eventos_api/")

print("Gerador de JSON simulando API export (descomente para executar)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gerar dados Parquet simulando export de MongoDB

# COMMAND ----------

# from faker import Faker
# from pyspark.sql.types import *
#
# fake = Faker("pt_BR")
#
# # Simular documentos MongoDB exportados como Parquet
# rows = []
# for i in range(800):
#     rows.append((
#         f"DOC-{i:05d}",
#         fake.name(),
#         fake.email(),
#         fake.city(),
#         fake.state_abbr(),
#         fake.random_element(["ativo", "inativo", "pendente"]),
#         fake.date_between(start_date="-2y", end_date="today").isoformat()
#     ))
#
# schema = StructType([
#     StructField("_id", StringType()),
#     StructField("nome", StringType()),
#     StructField("email", StringType()),
#     StructField("cidade", StringType()),
#     StructField("estado", StringType()),
#     StructField("status", StringType()),
#     StructField("data_cadastro", StringType())
# ])
#
# df_mongo = spark.createDataFrame(rows, schema)
# df_mongo.write.mode("overwrite").parquet("/tmp/landing/clientes_mongo/")
#
# print("Dados Parquet simulando MongoDB export gerados")

print("Gerador de Parquet simulando MongoDB (descomente para executar)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 6.2 — Pipeline: Auto Loader → Bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingestão 1: CSV → Bronze (vendas do ERP)

# COMMAND ----------

# --- Auto Loader: CSV → Bronze ---

# df_vendas_stream = (
#     spark.readStream
#     .format("cloudFiles")
#     .option("cloudFiles.format", "csv")
#     .option("cloudFiles.inferColumnTypes", "true")
#     .option("cloudFiles.schemaLocation", "/tmp/autoloader/schema/vendas_csv")
#     .option("header", "true")
#     .load("/tmp/landing/vendas_csv/*/")
# )
#
# # Adicionar metadados de ingestão
# df_vendas_bronze = df_vendas_stream \
#     .withColumn("_ingest_timestamp", current_timestamp()) \
#     .withColumn("_source_file", input_file_name()) \
#     .withColumn("_source_system", lit("erp_csv"))
#
# # Escrever na Bronze
# df_vendas_bronze.writeStream \
#     .format("delta") \
#     .option("checkpointLocation", "/tmp/autoloader/checkpoint/vendas_csv") \
#     .trigger(availableNow=True) \
#     .outputMode("append") \
#     .toTable("bronze.vendas_erp")

print("Pipeline CSV → Bronze com Auto Loader (descomente para executar)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingestão 2: JSON → Bronze (eventos da API)

# COMMAND ----------

# --- Auto Loader: JSON → Bronze ---

# df_eventos_stream = (
#     spark.readStream
#     .format("cloudFiles")
#     .option("cloudFiles.format", "json")
#     .option("cloudFiles.inferColumnTypes", "true")
#     .option("cloudFiles.schemaLocation", "/tmp/autoloader/schema/eventos_api")
#     .load("/tmp/landing/eventos_api/")
# )
#
# df_eventos_bronze = df_eventos_stream \
#     .withColumn("_ingest_timestamp", current_timestamp()) \
#     .withColumn("_source_file", input_file_name()) \
#     .withColumn("_source_system", lit("api_eventos"))
#
# df_eventos_bronze.writeStream \
#     .format("delta") \
#     .option("checkpointLocation", "/tmp/autoloader/checkpoint/eventos_api") \
#     .trigger(availableNow=True) \
#     .outputMode("append") \
#     .toTable("bronze.eventos_api")

print("Pipeline JSON → Bronze com Auto Loader (descomente para executar)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingestão 3: Parquet → Bronze (clientes do MongoDB)

# COMMAND ----------

# --- Auto Loader: Parquet → Bronze ---

# df_clientes_stream = (
#     spark.readStream
#     .format("cloudFiles")
#     .option("cloudFiles.format", "parquet")
#     .option("cloudFiles.schemaLocation", "/tmp/autoloader/schema/clientes_mongo")
#     .load("/tmp/landing/clientes_mongo/")
# )
#
# df_clientes_bronze = df_clientes_stream \
#     .withColumn("_ingest_timestamp", current_timestamp()) \
#     .withColumn("_source_file", input_file_name()) \
#     .withColumn("_source_system", lit("mongodb_export"))
#
# df_clientes_bronze.writeStream \
#     .format("delta") \
#     .option("checkpointLocation", "/tmp/autoloader/checkpoint/clientes_mongo") \
#     .trigger(availableNow=True) \
#     .outputMode("append") \
#     .toTable("bronze.clientes_mongo")

print("Pipeline Parquet → Bronze com Auto Loader (descomente para executar)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingestão 4: JDBC → Bronze (dados do SQL Server)

# COMMAND ----------

# --- JDBC → Bronze (batch, não streaming) ---

# jdbc_url = "jdbc:sqlserver://host:1433;database=erp"
# props = {
#     "user": dbutils.secrets.get(scope="jdbc", key="user"),
#     "password": dbutils.secrets.get(scope="jdbc", key="pass"),
#     "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
# }
#
# # Leitura incremental com marca d'água
# # Ler último timestamp processado
# try:
#     ultimo_ts = spark.sql("SELECT MAX(_ingest_timestamp) FROM bronze.pedidos_jdbc").first()[0]
# except:
#     ultimo_ts = "1900-01-01"
#
# df_jdbc = spark.read.jdbc(
#     url=jdbc_url,
#     table=f"(SELECT * FROM dbo.pedidos WHERE updated_at > '{ultimo_ts}') AS incremental",
#     properties=props
# )
#
# # Adicionar metadados e salvar
# df_jdbc \
#     .withColumn("_ingest_timestamp", current_timestamp()) \
#     .withColumn("_source_system", lit("sqlserver_erp")) \
#     .write.format("delta") \
#     .mode("append") \
#     .saveAsTable("bronze.pedidos_jdbc")

print("Pipeline JDBC → Bronze com ingestão incremental (descomente para executar)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 6.3 — Validação da Bronze

# COMMAND ----------

# --- Validação: contar registros em cada tabela bronze ---

# tabelas_bronze = [
#     "bronze.vendas_erp",
#     "bronze.eventos_api",
#     "bronze.clientes_mongo",
#     "bronze.pedidos_jdbc"
# ]
#
# for tabela in tabelas_bronze:
#     try:
#         count = spark.table(tabela).count()
#         print(f"✅ {tabela}: {count:,} registros")
#     except Exception as e:
#         print(f"❌ {tabela}: {e}")

print("Validação das tabelas bronze (descomente para executar)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Resumo: Padrões de Ingestão
# MAGIC
# MAGIC | Fonte | Método de Ingestão | Formato Landing | Bronze Method |
# MAGIC |-------|-------------------|----------------|---------------|
# MAGIC | **Cloud Storage** | Direto / External Location | CSV, JSON, Parquet | Auto Loader |
# MAGIC | **Banco Relacional** | JDBC | N/A (direto) | Spark JDBC Read |
# MAGIC | **NoSQL** | Export → Storage | JSON, Parquet | Auto Loader |
# MAGIC | **API REST** | requests → Storage | JSON | Auto Loader |
# MAGIC | **Kafka/Streaming** | Spark Streaming | Avro, JSON | Structured Streaming |
# MAGIC
# MAGIC ### Colunas de Metadados na Bronze
# MAGIC
# MAGIC | Coluna | Descrição |
# MAGIC |--------|-----------|
# MAGIC | `_ingest_timestamp` | Quando o dado foi ingerido |
# MAGIC | `_source_file` | Arquivo de origem (`input_file_name()`) |
# MAGIC | `_source_system` | Sistema fonte (erp, api, mongodb, etc.) |
# MAGIC | `_rescued_data` | Dados que não encaixaram no schema (Auto Loader) |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercícios

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício A — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Qual é a principal vantagem de adicionar `_ingest_timestamp` e `_source_file`
# MAGIC nas tabelas bronze?
# MAGIC
# MAGIC - a) Melhora a performance de leitura
# MAGIC - b) Permite auditoria e rastreabilidade dos dados
# MAGIC - c) Reduz o tamanho dos arquivos Delta
# MAGIC - d) É obrigatório para o Auto Loader funcionar
# MAGIC
# MAGIC > _Sua resposta: ___
# MAGIC >
# MAGIC > **Gabarito:** b) Metadados de ingestão permitem rastrear de onde e quando cada dado chegou.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício B — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Um pipeline usa Auto Loader para ingerir CSVs de uma landing zone.
# MAGIC Um novo arquivo chega com uma coluna extra que não existia antes.
# MAGIC O que acontece com a coluna extra se `cloudFiles.schemaEvolutionMode = "addNewColumns"`?
# MAGIC
# MAGIC - a) O stream falha com erro de schema
# MAGIC - b) A coluna extra é ignorada silenciosamente
# MAGIC - c) O schema é atualizado automaticamente e a coluna é adicionada
# MAGIC - d) Os dados são salvos na coluna `_rescued_data`
# MAGIC
# MAGIC > _Sua resposta: ___
# MAGIC >
# MAGIC > **Gabarito:** c) Com `addNewColumns`, o Auto Loader evolui o schema automaticamente.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício C — Desafio Prático
# MAGIC
# MAGIC Monte o pipeline completo usando os geradores de dados deste notebook:
# MAGIC
# MAGIC 1. Descomente e execute os geradores (seção 6.1)
# MAGIC 2. Descomente e execute os pipelines Auto Loader (seção 6.2)
# MAGIC 3. Execute a validação (seção 6.3)
# MAGIC 4. Verifique se todas as tabelas bronze foram criadas
# MAGIC 5. Faça uma query que una dados de 2+ fontes:
# MAGIC
# MAGIC ```sql
# MAGIC -- Exemplo: vendas + clientes
# MAGIC SELECT v.*, c.nome, c.email
# MAGIC FROM bronze.vendas_erp v
# MAGIC LEFT JOIN bronze.clientes_mongo c ON v.vendedor = c.nome
# MAGIC LIMIT 20;
# MAGIC ```
