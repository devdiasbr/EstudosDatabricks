# Databricks notebook source

# MAGIC %md
# MAGIC # Exercício 5 — Funções Avançadas: Explode, Pivot, Higher-Order
# MAGIC
# MAGIC **Módulo 3 — ELT com PySpark e SparkSQL**
# MAGIC
# MAGIC **Objetivo:** Dominar funções avançadas que aparecem no exame.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.1 — EXPLODE (Array → Linhas)

# COMMAND ----------

from pyspark.sql.functions import explode, split, array, col

# Dados com arrays
dados_tags = [
    (1, "Notebook", "eletronicos,informatica,tech"),
    (2, "Mouse", "perifericos,informatica"),
    (3, "Cadeira", "moveis,escritorio,ergonomia")
]
df_tags = spark.createDataFrame(dados_tags, ["id", "produto", "tags_str"])

# Transformar string em array e depois explodir
df_exploded = df_tags \
    .withColumn("tags_array", split(col("tags_str"), ",")) \
    .withColumn("tag", explode("tags_array"))

display(df_exploded.select("id", "produto", "tag"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Mesmo com SQL
# MAGIC SELECT id, produto, tag
# MAGIC FROM (
# MAGIC   SELECT id, produto, split(tags_str, ',') as tags
# MAGIC   FROM VALUES
# MAGIC     (1, 'Notebook', 'eletronicos,informatica,tech'),
# MAGIC     (2, 'Mouse', 'perifericos,informatica')
# MAGIC   AS t(id, produto, tags_str)
# MAGIC )
# MAGIC LATERAL VIEW explode(tags) AS tag;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.2 — PIVOT (Linhas → Colunas)

# COMMAND ----------

dados_vendas = [
    ("Notebook", "Jan", 5), ("Notebook", "Fev", 3), ("Notebook", "Mar", 7),
    ("Mouse", "Jan", 20), ("Mouse", "Fev", 15), ("Mouse", "Mar", 25),
    ("Teclado", "Jan", 10), ("Teclado", "Fev", 8), ("Teclado", "Mar", 12)
]
df_vendas = spark.createDataFrame(dados_vendas, ["produto", "mes", "quantidade"])

# Pivot: meses como colunas
df_pivot = df_vendas.groupBy("produto").pivot("mes", ["Jan", "Fev", "Mar"]).sum("quantidade")
display(df_pivot)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.3 — UNPIVOT / STACK (Colunas → Linhas)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Stack: inverso do pivot
# MAGIC SELECT produto, mes, quantidade
# MAGIC FROM (
# MAGIC   SELECT produto, stack(3, 'Jan', Jan, 'Fev', Fev, 'Mar', Mar) AS (mes, quantidade)
# MAGIC   FROM (
# MAGIC     SELECT 'Notebook' as produto, 5 as Jan, 3 as Fev, 7 as Mar
# MAGIC     UNION ALL
# MAGIC     SELECT 'Mouse', 20, 15, 25
# MAGIC   )
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.4 — Higher-Order Functions

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TRANSFORM: aplicar função a cada elemento do array
# MAGIC SELECT transform(array(1, 2, 3, 4, 5), x -> x * 2) AS dobrado;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- FILTER: filtrar elementos do array
# MAGIC SELECT filter(array(1, 2, 3, 4, 5, 6), x -> x > 3) AS maiores_que_3;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- EXISTS: verificar se algum elemento atende a condição
# MAGIC SELECT exists(array(1, 2, 3), x -> x = 2) AS contem_2;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- REDUCE: reduzir array a um único valor
# MAGIC SELECT reduce(array(1, 2, 3, 4, 5), 0, (acc, x) -> acc + x) AS soma;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.5 — Trabalhando com JSON/Structs Aninhados

# COMMAND ----------

from pyspark.sql.functions import from_json, get_json_object, schema_of_json

# Dados com JSON embutido
dados_json = [
    (1, '{"nome": "Ana", "endereco": {"cidade": "SP", "cep": "01000"}}'),
    (2, '{"nome": "Bruno", "endereco": {"cidade": "RJ", "cep": "20000"}}')
]
df_json = spark.createDataFrame(dados_json, ["id", "payload"])

# Extrair campos do JSON
df_parsed = df_json \
    .withColumn("nome", get_json_object("payload", "$.nome")) \
    .withColumn("cidade", get_json_object("payload", "$.endereco.cidade"))

display(df_parsed)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercícios
# MAGIC
# MAGIC ### Exercício A — Explode
# MAGIC Dado um DataFrame com uma coluna `interesses` (array de strings), use explode para criar uma linha por interesse. Conte quantas vezes cada interesse aparece.

# COMMAND ----------

# TODO: Seu código aqui
# dados = [(1, ["python", "spark", "sql"]), (2, ["spark", "delta", "python"]), ...]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício B — Pivot
# MAGIC Crie um DataFrame de vendas por estado e trimestre. Use pivot para ter os trimestres como colunas.

# COMMAND ----------

# TODO: Seu código aqui

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício C — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Qual função SQL do Spark transforma um array em múltiplas linhas?
# MAGIC
# MAGIC - a) `flatten()`
# MAGIC - b) `split()`
# MAGIC - c) `explode()`
# MAGIC - d) `collect_list()`
# MAGIC
# MAGIC > _Sua resposta: ___

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício D — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Qual higher-order function retorna um novo array contendo apenas os elementos que atendem a uma condição?
# MAGIC
# MAGIC - a) `transform()`
# MAGIC - b) `filter()`
# MAGIC - c) `exists()`
# MAGIC - d) `reduce()`
# MAGIC
# MAGIC > _Sua resposta: ___

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpoint
# MAGIC
# MAGIC - [ ] explode: array → múltiplas linhas
# MAGIC - [ ] pivot: linhas → colunas
# MAGIC - [ ] stack/unpivot: colunas → linhas
# MAGIC - [ ] Higher-order: transform, filter, exists, reduce
# MAGIC - [ ] JSON: get_json_object, from_json
# MAGIC - [ ] Exercícios A, B, C e D completados
