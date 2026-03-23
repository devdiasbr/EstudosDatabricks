# Databricks notebook source

# MAGIC %md
# MAGIC # Exercício 3 — PySpark vs SparkSQL: Lado a Lado
# MAGIC
# MAGIC **Módulo 3 — ELT com PySpark e SparkSQL**
# MAGIC
# MAGIC **Objetivo:** Comparar a mesma operação nas duas sintaxes — essencial para o exame.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup — Carregar dados do Faker
# MAGIC
# MAGIC > **Pré-requisito:** Execute `utils/gerar_dados_faker.py` antes de começar.

# COMMAND ----------

# Usar a tabela de funcionários gerada pelo Faker
df = spark.table("certificacao_bronze.funcionarios")
df.createOrReplaceTempView("funcionarios")

print(f"funcionarios: {df.count():,} registros")
display(df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comparativo: SELECT

# COMMAND ----------

# PySpark
df.select("nome", "departamento", "salario").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SparkSQL
# MAGIC SELECT nome, departamento, salario FROM funcionarios;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comparativo: FILTER / WHERE

# COMMAND ----------

# PySpark
from pyspark.sql.functions import col
df.filter(col("salario") > 9000).show()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SparkSQL
# MAGIC SELECT * FROM funcionarios WHERE salario > 9000;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comparativo: GROUP BY

# COMMAND ----------

# PySpark
from pyspark.sql.functions import count, avg, round as spark_round
df.groupBy("departamento") \
    .agg(count("*").alias("total"), spark_round(avg("salario"), 2).alias("media")) \
    .show()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SparkSQL
# MAGIC SELECT departamento, COUNT(*) as total, ROUND(AVG(salario), 2) as media
# MAGIC FROM funcionarios
# MAGIC GROUP BY departamento;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comparativo: ORDER BY

# COMMAND ----------

# PySpark
df.orderBy(col("salario").desc()).show()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SparkSQL
# MAGIC SELECT * FROM funcionarios ORDER BY salario DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comparativo: withColumn / CASE WHEN

# COMMAND ----------

# PySpark
from pyspark.sql.functions import when
df.withColumn("faixa", when(col("salario") >= 9000, "Senior").otherwise("Pleno")).show()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SparkSQL
# MAGIC SELECT *,
# MAGIC   CASE WHEN salario >= 9000 THEN 'Senior' ELSE 'Pleno' END AS faixa
# MAGIC FROM funcionarios;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercício — Complete a tabela
# MAGIC
# MAGIC | Operação | PySpark | SparkSQL |
# MAGIC |----------|---------|----------|
# MAGIC | Selecionar colunas | `df.select("col1", "col2")` | `SELECT col1, col2 FROM tabela` |
# MAGIC | Filtrar | `df.filter(col("x") > 10)` | ___ |
# MAGIC | Agrupar | ___ | `SELECT col, COUNT(*) FROM t GROUP BY col` |
# MAGIC | Ordenar | `df.orderBy(col("x").desc())` | ___ |
# MAGIC | Criar coluna | `df.withColumn("nova", ...)` | ___ |
# MAGIC | Renomear | `df.withColumnRenamed("old", "new")` | ___ |
# MAGIC | Remover coluna | ___ | Não aplicável diretamente |
# MAGIC | Distinct | ___ | `SELECT DISTINCT col FROM t` |
# MAGIC | Limitar | `df.limit(10)` | ___ |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exam-Style
# MAGIC
# MAGIC **Pergunta:** Qual a diferença entre uma Temp View e uma Global Temp View?
# MAGIC Em qual database a Global Temp View é registrada?
# MAGIC
# MAGIC > _Sua resposta aqui..._
