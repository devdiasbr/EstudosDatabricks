# Databricks notebook source

# MAGIC %md
# MAGIC # Exercício 1 — Setup do Ambiente de Estudos
# MAGIC
# MAGIC **Módulo 2 — Ambiente Prático**
# MAGIC
# MAGIC **Vídeo de apoio:** [Workspace na Prática](https://www.youtube.com/playlist?list=PLnxUgOpvXnWzTkha0174lXfES019PQz9U) — Vídeo 4
# MAGIC
# MAGIC **Objetivo:** Configurar o ambiente de trabalho para os exercícios do curso.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 — Verificar o ambiente

# COMMAND ----------

# Verificar versão do Spark e Runtime
print(f"Spark Version: {spark.version}")
print(f"Runtime: {spark.conf.get('spark.databricks.clusterUsageTags.sparkVersion', 'N/A')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 — Listar databases existentes

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW DATABASES;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.3 — Explorar datasets de exemplo

# COMMAND ----------

# Listar datasets disponíveis no Databricks
display(dbutils.fs.ls("/databricks-datasets/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.4 — Criar estrutura de pastas para o curso
# MAGIC
# MAGIC Execute as células abaixo para criar a estrutura de databases:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Criar database para cada camada
# MAGIC CREATE DATABASE IF NOT EXISTS certificacao_bronze
# MAGIC COMMENT 'Camada bronze - dados brutos';
# MAGIC
# MAGIC CREATE DATABASE IF NOT EXISTS certificacao_silver
# MAGIC COMMENT 'Camada silver - dados limpos';
# MAGIC
# MAGIC CREATE DATABASE IF NOT EXISTS certificacao_gold
# MAGIC COMMENT 'Camada gold - dados agregados';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verificar databases criados
# MAGIC SHOW DATABASES LIKE 'certificacao*';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.5 — Explorar um dataset de exemplo

# COMMAND ----------

# Carregar dataset de exemplo
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv")

print(f"Total de registros: {df.count()}")
print(f"Total de colunas: {len(df.columns)}")
df.printSchema()

# COMMAND ----------

display(df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.6 — Testar dbutils
# MAGIC
# MAGIC O `dbutils` é essencial para o dia a dia do Data Engineer:

# COMMAND ----------

# Explorar os módulos do dbutils
dbutils.help()

# COMMAND ----------

# File system utilities
dbutils.fs.help()

# COMMAND ----------

# Listar arquivos
display(dbutils.fs.ls("/databricks-datasets/learning-spark-v2/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpoint
# MAGIC
# MAGIC - [ ] Ambiente configurado e funcionando
# MAGIC - [ ] Databases certificacao_bronze, silver e gold criados
# MAGIC - [ ] Dataset de exemplo carregado com sucesso
# MAGIC - [ ] dbutils funcionando corretamente
