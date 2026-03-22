# Databricks notebook source

# MAGIC %md
# MAGIC # Exercício 1 — Leitura de Dados em Múltiplos Formatos
# MAGIC
# MAGIC **Módulo 3 — ELT com PySpark e SparkSQL**
# MAGIC
# MAGIC **Vídeo de apoio:** [Processando dados com PySpark e SparkSQL](https://www.youtube.com/playlist?list=PLnxUgOpvXnWzTkha0174lXfES019PQz9U) — Vídeo 7
# MAGIC
# MAGIC **Objetivo:** Dominar a leitura de dados nos formatos mais comuns.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 — Leitura de CSV

# COMMAND ----------

# Leitura básica — sem opções
df_csv_basico = spark.read.format("csv") \
    .load("/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv")

display(df_csv_basico.limit(5))
# Observe: sem header e sem schema correto

# COMMAND ----------

# Leitura com opções corretas
df_csv = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("sep", ",") \
    .load("/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv")

print(f"Registros: {df_csv.count()}")
df_csv.printSchema()
display(df_csv.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 — Leitura de JSON

# COMMAND ----------

# JSON simples
df_json = spark.read.format("json") \
    .option("multiLine", "true") \
    .load("/databricks-datasets/learning-spark-v2/iot-devices/iot_devices.json")

print(f"Registros: {df_json.count()}")
df_json.printSchema()
display(df_json.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.3 — Leitura de Parquet

# COMMAND ----------

# Parquet — formato colunar (não precisa inferSchema, está embutido)
df_parquet = spark.read.format("parquet") \
    .load("/databricks-datasets/learning-spark-v2/flights/summary-data/parquet/2010-summary.parquet")

print(f"Registros: {df_parquet.count()}")
df_parquet.printSchema()
display(df_parquet.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.4 — Leitura de Delta

# COMMAND ----------

# Delta — formato nativo do Lakehouse
df_delta = spark.read.format("delta") \
    .load("/databricks-datasets/learning-spark-v2/people/people-10m.delta")

print(f"Registros: {df_delta.count()}")
df_delta.printSchema()
display(df_delta.limit(5))

# COMMAND ----------

# Leitura Delta via SQL
# MAGIC %sql
# MAGIC SELECT * FROM delta.`/databricks-datasets/learning-spark-v2/people/people-10m.delta` LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.5 — Leitura com Schema Explícito (StructType)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Definir schema manualmente
schema_vendas = StructType([
    StructField("id", IntegerType(), False),
    StructField("produto", StringType(), True),
    StructField("categoria", StringType(), True),
    StructField("preco", DoubleType(), True),
    StructField("quantidade", IntegerType(), True)
])

# Criar dados de exemplo para usar com schema explícito
dados = [
    (1, "Notebook", "Eletrônicos", 3500.00, 2),
    (2, "Mouse", "Periféricos", 89.90, 10),
    (3, "Teclado", "Periféricos", 199.90, 5),
    (4, "Monitor", "Eletrônicos", 1899.00, 3),
    (5, "Webcam", "Periféricos", 299.90, 8)
]

df_schema = spark.createDataFrame(dados, schema_vendas)
df_schema.printSchema()
display(df_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercícios para Praticar
# MAGIC
# MAGIC ### Exercício A
# MAGIC Encontre outro dataset em `/databricks-datasets/` e leia em qualquer formato.
# MAGIC Imprima: total de registros, total de colunas, e o schema.

# COMMAND ----------

# TODO: Seu código aqui
# display(dbutils.fs.ls("/databricks-datasets/"))
# ...

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício B — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Qual opção é necessária ao ler um arquivo CSV com cabeçalho para que o Spark use a primeira linha como nomes de coluna?
# MAGIC
# MAGIC - a) `.option("schema", "true")`
# MAGIC - b) `.option("header", "true")`
# MAGIC - c) `.option("columns", "first")`
# MAGIC - d) `.option("firstRowAsHeader", "true")`
# MAGIC
# MAGIC > _Sua resposta: ___

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício C — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Qual formato de arquivo NÃO requer `inferSchema` porque o schema já está embutido no arquivo?
# MAGIC
# MAGIC - a) CSV
# MAGIC - b) JSON
# MAGIC - c) Parquet
# MAGIC - d) Texto
# MAGIC
# MAGIC > _Sua resposta: ___

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpoint
# MAGIC
# MAGIC - [ ] CSV lido com header e inferSchema
# MAGIC - [ ] JSON lido com multiLine
# MAGIC - [ ] Parquet lido sem necessidade de schema
# MAGIC - [ ] Delta lido via PySpark e via SQL
# MAGIC - [ ] Schema explícito criado com StructType
# MAGIC - [ ] Exercícios A, B e C completados
