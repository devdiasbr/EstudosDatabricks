# Databricks notebook source

# MAGIC %md
# MAGIC # Exercício 2 — Delta Live Tables (DLT): Pipeline Medallion
# MAGIC
# MAGIC **Módulo 5 — Pipelines de Produção**
# MAGIC
# MAGIC **Vídeo de apoio:** [Delta Live Tables](https://www.youtube.com/playlist?list=PLnxUgOpvXnWzTkha0174lXfES019PQz9U) — Vídeo 11
# MAGIC
# MAGIC **Objetivo:** Criar um pipeline declarativo com qualidade de dados.
# MAGIC
# MAGIC > **NOTA:** Este notebook deve ser executado como um pipeline DLT, não interativamente.
# MAGIC > No Databricks, vá em **Workflows → Delta Live Tables → Create Pipeline** e aponte para este notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conceitos-chave: DLT
# MAGIC
# MAGIC | Conceito | Descrição |
# MAGIC |---------|-----------|
# MAGIC | `@dlt.table` | Define uma tabela no pipeline |
# MAGIC | `@dlt.view` | Define uma view temporária (não persistida) |
# MAGIC | `dlt.read()` | Lê de outra tabela DLT (batch) |
# MAGIC | `dlt.read_stream()` | Lê como stream de outra tabela DLT |
# MAGIC | `@dlt.expect()` | Expectation: registra violações mas mantém registros |
# MAGIC | `@dlt.expect_or_drop()` | Expectation: remove registros que violam |
# MAGIC | `@dlt.expect_or_fail()` | Expectation: falha o pipeline se houver violação |

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, upper, current_timestamp, to_date, sum as spark_sum

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze — Ingestão com Auto Loader

# COMMAND ----------

@dlt.table(
    comment="Dados brutos de vendas ingeridos via Auto Loader"
)
def bronze_vendas():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .load("/data/vendas/")  # Ajuste o path para seus dados
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver — Limpeza com Expectations

# COMMAND ----------

@dlt.table(
    comment="Vendas limpas e validadas"
)
@dlt.expect("id_nao_nulo", "id IS NOT NULL")
@dlt.expect_or_drop("valor_positivo", "valor > 0")
@dlt.expect_or_drop("produto_nao_nulo", "produto IS NOT NULL")
def silver_vendas():
    return (
        dlt.read_stream("bronze_vendas")
        .withColumn("categoria", upper(col("categoria")))
        .withColumn("data_venda", to_date(col("data_venda"), "yyyy-MM-dd"))
        .withColumn("processado_em", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold — Agregações

# COMMAND ----------

@dlt.table(
    comment="Receita diária por categoria"
)
def gold_vendas_diarias():
    return (
        dlt.read("silver_vendas")
        .groupBy("data_venda", "categoria")
        .agg(
            spark_sum("valor").alias("receita_total"),
            spark_sum("quantidade").alias("total_itens")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Exercícios (responder no módulo de simulados)
# MAGIC
# MAGIC ### Exercício A — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Qual a diferença entre `@dlt.expect`, `@dlt.expect_or_drop` e `@dlt.expect_or_fail`?
# MAGIC
# MAGIC | Decorator | Registro inválido | Pipeline |
# MAGIC |-----------|-------------------|----------|
# MAGIC | `expect` | ___ | ___ |
# MAGIC | `expect_or_drop` | ___ | ___ |
# MAGIC | `expect_or_fail` | ___ | ___ |
# MAGIC
# MAGIC ### Exercício B — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Qual a diferença entre `dlt.read()` e `dlt.read_stream()`?
# MAGIC
# MAGIC > _Sua resposta..._
# MAGIC
# MAGIC ### Exercício C — Exam-Style
# MAGIC
# MAGIC **Pergunta:** O que é Auto Loader (`cloudFiles`) e qual sua vantagem sobre `spark.readStream.format('csv')`?
# MAGIC
# MAGIC > _Sua resposta..._
# MAGIC
# MAGIC ### Exercício D
# MAGIC Crie uma tabela comparativa: **Streaming Table vs Materialized View no DLT**
# MAGIC
# MAGIC | Característica | Streaming Table | Materialized View |
# MAGIC |---------------|-----------------|-------------------|
# MAGIC | Tipo de leitura | ___ | ___ |
# MAGIC | Reprocessamento | ___ | ___ |
# MAGIC | Uso ideal | ___ | ___ |
