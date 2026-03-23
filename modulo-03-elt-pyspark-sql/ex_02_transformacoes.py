# Databricks notebook source

# MAGIC %md
# MAGIC # Exercício 2 — Transformações com PySpark
# MAGIC
# MAGIC **Módulo 3 — ELT com PySpark e SparkSQL**
# MAGIC
# MAGIC **Objetivo:** Dominar as transformações essenciais cobradas no exame.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup — Carregar dados do Faker
# MAGIC
# MAGIC > **Pré-requisito:** Execute `utils/gerar_dados_faker.py` antes de começar.

# COMMAND ----------

from pyspark.sql.functions import col, lit, when, upper, round as spark_round, current_date, datediff, to_date

# Carregar tabelas geradas pelo Faker
df_clientes = spark.table("certificacao_bronze.clientes")
df_pedidos  = spark.table("certificacao_bronze.vendas")

df_clientes.createOrReplaceTempView("clientes")
df_pedidos.createOrReplaceTempView("pedidos")

print(f"clientes: {df_clientes.count():,} registros")
print(f"vendas  : {df_pedidos.count():,} registros")
display(df_clientes.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 — SELECT e Renomear Colunas

# COMMAND ----------

# select + alias
df_clientes.select("id", "nome", col("estado").alias("uf")).show()

# COMMAND ----------

# selectExpr — permite expressões SQL
df_clientes.selectExpr("id", "nome", "upper(estado) as uf_upper").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 — FILTER / WHERE

# COMMAND ----------

# Filtrar clientes de SP
df_clientes.filter(col("estado") == "SP").show()

# Filtrar com múltiplas condições
df_pedidos.filter((col("valor") > 200) & (col("valor") < 2000)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 — withColumn (Criar/Modificar Colunas)

# COMMAND ----------

# Adicionar colunas
df_enriquecido = df_pedidos \
    .withColumn("imposto", spark_round(col("valor") * 0.15, 2)) \
    .withColumn("valor_total", spark_round(col("valor") * 1.15, 2)) \
    .withColumn("status", when(col("valor") > 1000, "ALTO").otherwise("NORMAL"))

display(df_enriquecido)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.4 — GROUP BY + Agregações

# COMMAND ----------

from pyspark.sql.functions import count, sum as spark_sum, avg, min as spark_min, max as spark_max

df_pedidos.groupBy("cliente_id") \
    .agg(
        count("*").alias("total_pedidos"),
        spark_round(spark_sum("valor"), 2).alias("valor_total"),
        spark_round(avg("valor"), 2).alias("ticket_medio"),
        spark_min("valor").alias("menor_valor"),
        spark_max("valor").alias("maior_valor")
    ) \
    .orderBy(col("valor_total").desc()) \
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.5 — JOINs

# COMMAND ----------

# INNER JOIN — só clientes com pedidos
df_inner = df_clientes.join(df_pedidos, df_clientes.id == df_pedidos.cliente_id, "inner")
print(f"INNER JOIN: {df_inner.count()} registros")
display(df_inner.select("nome", "produto", "valor"))

# COMMAND ----------

# LEFT JOIN — todos os clientes, com ou sem pedidos
df_left = df_clientes.join(df_pedidos, df_clientes.id == df_pedidos.cliente_id, "left")
print(f"LEFT JOIN: {df_left.count()} registros")
display(df_left.select("nome", "produto", "valor"))

# COMMAND ----------

# ANTI JOIN — clientes SEM pedidos
df_anti = df_clientes.join(df_pedidos, df_clientes.id == df_pedidos.cliente_id, "anti")
print(f"ANTI JOIN (clientes sem pedidos): {df_anti.count()}")
display(df_anti)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.6 — ORDER BY

# COMMAND ----------

df_pedidos.orderBy(col("valor").desc()).show()
df_pedidos.orderBy("produto", col("valor").asc()).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercícios para Praticar
# MAGIC
# MAGIC ### Exercício A
# MAGIC Usando `df_clientes`, crie uma coluna `dias_como_cliente` que calcula quantos dias se passaram desde o cadastro.

# COMMAND ----------

# TODO: Seu código aqui
# df_clientes.withColumn("dias_como_cliente", ...)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício B
# MAGIC Faça um LEFT JOIN entre pedidos e clientes. Encontre o pedido cujo `cliente_id` não tem correspondência (pedido órfão).

# COMMAND ----------

# TODO: Seu código aqui

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício C
# MAGIC Agrupe pedidos por mês (`data_pedido`) e calcule: total de pedidos, soma de valores e ticket médio.

# COMMAND ----------

# TODO: Seu código aqui
# from pyspark.sql.functions import month
# ...

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício D — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Qual tipo de JOIN retorna apenas os registros da tabela esquerda que NÃO têm correspondência na tabela direita?
# MAGIC
# MAGIC - a) INNER JOIN
# MAGIC - b) LEFT JOIN
# MAGIC - c) ANTI JOIN
# MAGIC - d) CROSS JOIN
# MAGIC
# MAGIC > _Sua resposta: ___

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpoint
# MAGIC
# MAGIC - [ ] select, selectExpr e alias
# MAGIC - [ ] filter com condições simples e compostas
# MAGIC - [ ] withColumn com when/otherwise
# MAGIC - [ ] groupBy com múltiplas agregações
# MAGIC - [ ] JOINs: inner, left, anti
# MAGIC - [ ] orderBy asc e desc
# MAGIC - [ ] Exercícios A, B, C e D completados
