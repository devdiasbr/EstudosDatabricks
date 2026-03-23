# Databricks notebook source

# MAGIC %md
# MAGIC # Exercício 3 — Conexão com Bancos NoSQL
# MAGIC
# MAGIC **Módulo 05-B — Conexões com Fontes Externas**
# MAGIC
# MAGIC **Objetivo:** Conectar o Databricks a MongoDB, Azure Cosmos DB e AWS DynamoDB.
# MAGIC Entender conectores, formatos e diferenças em relação ao JDBC.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conceitos-chave: NoSQL no Databricks
# MAGIC
# MAGIC | Banco | Tipo | Conector Databricks | Formato Nativo |
# MAGIC |-------|------|--------------------|--------------------|
# MAGIC | **MongoDB** | Document Store | `mongodb-spark-connector` | BSON/JSON |
# MAGIC | **Cosmos DB** | Multi-model | `azure-cosmosdb-spark` / OLTP Connector | JSON |
# MAGIC | **DynamoDB** | Key-Value | EMR connector / Custom | JSON |
# MAGIC | **Cassandra** | Wide Column | `spark-cassandra-connector` | Tabular |
# MAGIC | **Redis** | Key-Value/Cache | `spark-redis` | Key-Value |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3.1 — MongoDB

# COMMAND ----------

# --- MongoDB ---
# Instalar: %pip install pymongo
# Ou usar o conector Spark oficial

# Método 1: Spark Connector (recomendado para big data)
# df = spark.read \
#     .format("mongodb") \
#     .option("connection.uri", dbutils.secrets.get(scope="nosql", key="mongo-uri")) \
#     .option("database", "ecommerce") \
#     .option("collection", "pedidos") \
#     .load()

# Método 2: Spark com formato antigo (mongo-spark-connector)
# df = spark.read \
#     .format("com.mongodb.spark.sql.DefaultSource") \
#     .option("uri", "mongodb+srv://user:pass@cluster.mongodb.net/") \
#     .option("database", "ecommerce") \
#     .option("collection", "pedidos") \
#     .load()

# Método 3: PyMongo (para volumes pequenos / validação)
# from pymongo import MongoClient
# client = MongoClient(dbutils.secrets.get(scope="nosql", key="mongo-uri"))
# db = client["ecommerce"]
# docs = list(db["pedidos"].find().limit(100))
# df = spark.createDataFrame(docs)

print("MongoDB — 3 métodos de conexão")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Desafio MongoDB: Nested Documents
# MAGIC
# MAGIC MongoDB armazena documentos aninhados (JSON). No Spark, eles viram `StructType`:
# MAGIC
# MAGIC ```json
# MAGIC {
# MAGIC   "_id": "abc123",
# MAGIC   "cliente": {
# MAGIC     "nome": "Maria",
# MAGIC     "endereco": {
# MAGIC       "cidade": "São Paulo",
# MAGIC       "estado": "SP"
# MAGIC     }
# MAGIC   },
# MAGIC   "itens": [
# MAGIC     {"produto": "Notebook", "valor": 3500.00},
# MAGIC     {"produto": "Mouse", "valor": 89.90}
# MAGIC   ]
# MAGIC }
# MAGIC ```

# COMMAND ----------

# Flatten nested documents do MongoDB
# from pyspark.sql.functions import col, explode

# df_flat = df.select(
#     col("_id"),
#     col("cliente.nome").alias("cliente_nome"),
#     col("cliente.endereco.cidade").alias("cidade"),
#     col("cliente.endereco.estado").alias("estado"),
#     explode(col("itens")).alias("item")
# ).select(
#     "*",
#     col("item.produto").alias("produto"),
#     col("item.valor").alias("valor")
# ).drop("item")

print("Flatten de documentos aninhados do MongoDB")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3.2 — Azure Cosmos DB

# COMMAND ----------

# --- Azure Cosmos DB ---

# Método 1: OLTP Connector (recomendado)
# spark.conf.set("spark.cosmos.accountEndpoint", dbutils.secrets.get(scope="nosql", key="cosmos-endpoint"))
# spark.conf.set("spark.cosmos.accountKey", dbutils.secrets.get(scope="nosql", key="cosmos-key"))

# df = spark.read \
#     .format("cosmos.oltp") \
#     .option("spark.cosmos.container", "pedidos") \
#     .option("spark.cosmos.database", "ecommerce") \
#     .option("spark.cosmos.read.inferSchema.enabled", "true") \
#     .load()

# Método 2: Analytical Store (Synapse Link) — para analytics pesados
# df = spark.read \
#     .format("cosmos.olap") \
#     .option("spark.cosmos.container", "pedidos") \
#     .option("spark.cosmos.database", "ecommerce") \
#     .load()

print("Cosmos DB — OLTP Connector e Analytical Store")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cosmos DB: OLTP vs OLAP (Analytical Store)
# MAGIC
# MAGIC | Aspecto | OLTP Connector | Analytical Store (OLAP) |
# MAGIC |---------|---------------|------------------------|
# MAGIC | Formato | `cosmos.oltp` | `cosmos.olap` |
# MAGIC | Uso | Transacional, CRUD | Analytics, agregações pesadas |
# MAGIC | Impacto no RU | ⚠️ Consome RU/s | ✅ Sem impacto no transacional |
# MAGIC | Latência | Baixa | Segundos (sync automático) |
# MAGIC | Recomendação | Leituras pontuais | Ingestão no Lakehouse |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3.3 — AWS DynamoDB

# COMMAND ----------

# --- AWS DynamoDB ---

# Método 1: Via EMR/Glue Connector
# df = spark.read \
#     .format("dynamodb") \
#     .option("tableName", "pedidos") \
#     .option("region", "us-east-1") \
#     .load()

# Método 2: Boto3 (para volumes pequenos)
# import boto3
# import json
#
# dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
# table = dynamodb.Table("pedidos")
# response = table.scan()
# items = response["Items"]
# df = spark.createDataFrame(items)

# Método 3: Export to S3 + Auto Loader (recomendado para big data)
# DynamoDB → Export to S3 (JSON) → Auto Loader → Delta Bronze
# Mais eficiente e não consome RCU do DynamoDB

print("DynamoDB — 3 métodos de conexão")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3.4 — Padrão Recomendado: NoSQL → Landing → Delta
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────┐     Export/CDC      ┌──────────────┐    Auto Loader    ┌──────────────┐
# MAGIC │   MongoDB    │ ──────────────────→ │  Landing S3  │ ────────────────→ │ Delta Bronze │
# MAGIC │  Cosmos DB   │   JSON/Parquet      │  ADLS / GCS  │   cloudFiles      │  (Lakehouse) │
# MAGIC │  DynamoDB    │                     │              │                   │              │
# MAGIC └──────────────┘                     └──────────────┘                   └──────────────┘
# MAGIC ```
# MAGIC
# MAGIC **Por que não ler direto?**
# MAGIC - Leituras diretas consomem recursos do banco operacional (RU, RCU)
# MAGIC - Export → Landing → Delta é mais eficiente e desacopla os sistemas
# MAGIC - Permite reprocessamento sem impactar o banco fonte

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercícios

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício A — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Qual é a abordagem MAIS recomendada para ingerir dados de um MongoDB
# MAGIC com 500GB no Databricks Lakehouse?
# MAGIC
# MAGIC - a) Ler direto com Spark Connector em tempo real
# MAGIC - b) Usar PyMongo para ler todos os documentos
# MAGIC - c) Exportar para Cloud Storage (JSON/Parquet) e usar Auto Loader
# MAGIC - d) Criar mount point apontando para o MongoDB
# MAGIC
# MAGIC > _Sua resposta: ___
# MAGIC >
# MAGIC > **Gabarito:** c) Export → Landing → Auto Loader desacopla os sistemas e não impacta o banco.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício B — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Qual vantagem do Cosmos DB Analytical Store (Synapse Link) sobre o OLTP Connector?
# MAGIC
# MAGIC - a) Menor latência de leitura
# MAGIC - b) Suporta mais tipos de dados
# MAGIC - c) Não consome Request Units (RU/s) do banco transacional
# MAGIC - d) Permite escrita de volta no Cosmos DB
# MAGIC
# MAGIC > _Sua resposta: ___
# MAGIC >
# MAGIC > **Gabarito:** c) O Analytical Store é uma cópia colunar separada, sem impacto no transacional.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício C — Prático: Flatten JSON
# MAGIC
# MAGIC Dado o DataFrame abaixo (simulando dados do MongoDB), faça o flatten:
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.types import *
# MAGIC
# MAGIC data = [
# MAGIC     ("1", {"nome": "Ana", "cidade": "SP"}, [{"produto": "Mouse", "valor": 89.9}]),
# MAGIC     ("2", {"nome": "Bob", "cidade": "RJ"}, [{"produto": "Teclado", "valor": 150.0}, {"produto": "Monitor", "valor": 1200.0}])
# MAGIC ]
# MAGIC
# MAGIC schema = StructType([
# MAGIC     StructField("_id", StringType()),
# MAGIC     StructField("cliente", StructType([
# MAGIC         StructField("nome", StringType()),
# MAGIC         StructField("cidade", StringType())
# MAGIC     ])),
# MAGIC     StructField("itens", ArrayType(StructType([
# MAGIC         StructField("produto", StringType()),
# MAGIC         StructField("valor", DoubleType())
# MAGIC     ])))
# MAGIC ])
# MAGIC
# MAGIC df = spark.createDataFrame(data, schema)
# MAGIC
# MAGIC # TODO: Faça o flatten para obter colunas: _id, nome, cidade, produto, valor
# MAGIC # Dica: use col("cliente.nome"), explode(col("itens"))
# MAGIC ```
