# Databricks notebook source

# MAGIC %md
# MAGIC # Exercício 4 — Pipeline ELT Completo: Bronze → Silver → Gold
# MAGIC
# MAGIC **Módulo 3 — ELT com PySpark e SparkSQL**
# MAGIC
# MAGIC **Vídeo de apoio:** [Processamento e Análise de Dados](https://www.youtube.com/playlist?list=PLnxUgOpvXnWzTkha0174lXfES019PQz9U) — Vídeo 9
# MAGIC
# MAGIC **Objetivo:** Implementar um pipeline ELT completo usando a arquitetura Medallion.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Arquitetura Medallion
# MAGIC
# MAGIC ```
# MAGIC ┌──────────┐     ┌──────────┐     ┌──────────┐
# MAGIC │  BRONZE  │ ──► │  SILVER  │ ──► │   GOLD   │
# MAGIC │  (Raw)   │     │ (Clean)  │     │  (Agg)   │
# MAGIC └──────────┘     └──────────┘     └──────────┘
# MAGIC   Ingestão        Limpeza          Agregação
# MAGIC   Dados brutos    Tipagem          Métricas
# MAGIC   Sem schema      Dedup            Dashboards
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — EXTRACT: Carregar dados brutos (Faker)
# MAGIC
# MAGIC > **Pré-requisito:** Execute `utils/gerar_dados_faker.py` antes de começar.
# MAGIC
# MAGIC A tabela `certificacao_bronze.vendas_raw` já foi gerada pelo Faker com problemas reais:
# MAGIC - produto nulo (~5%)
# MAGIC - valor negativo (~5%)
# MAGIC - estado nulo (~5%)
# MAGIC - data malformada (~5%)
# MAGIC - ~500 duplicatas

# COMMAND ----------

df_bruto = spark.table("certificacao_bronze.vendas_raw")

print(f"Registros brutos: {df_bruto.count():,}")
display(df_bruto.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — LOAD: Bronze já disponível
# MAGIC
# MAGIC A tabela Bronze foi criada pelo gerador de dados.

# COMMAND ----------

# Confirmar que a bronze existe
print(f"certificacao_bronze.vendas_raw: {spark.table('certificacao_bronze.vendas_raw').count():,} registros")
print("Bronze OK — pronto para limpeza.")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM certificacao_bronze.vendas_raw;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — TRANSFORM: Limpar dados e salvar na camada Silver

# COMMAND ----------

from pyspark.sql.functions import col, to_date, trim, lower, row_number
from pyspark.sql.window import Window

# Ler da Bronze
df_bronze = spark.table("certificacao_bronze.vendas_raw")

# --- Limpeza ---
df_silver = df_bronze \
    .filter(col("produto").isNotNull()) \
    .filter(col("estado").isNotNull()) \
    .withColumn("valor", col("valor").cast("double")) \
    .filter(col("valor") > 0) \
    .withColumn("data_venda", to_date(col("data_venda"), "yyyy-MM-dd")) \
    .withColumn("produto", trim(col("produto"))) \
    .withColumn("categoria", lower(trim(col("categoria"))))

# --- Remover duplicatas ---
window = Window.partitionBy("produto", "data_venda", "estado").orderBy("id")
df_silver = df_silver \
    .withColumn("rn", row_number().over(window)) \
    .filter(col("rn") == 1) \
    .drop("rn")

print(f"Bronze: {df_bronze.count()} → Silver: {df_silver.count()} registros")
display(df_silver)

# COMMAND ----------

# Salvar na Silver
df_silver.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("certificacao_silver.vendas_clean")

print("Silver salvo com sucesso!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — AGGREGATE: Criar métricas na camada Gold

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Gold 1: Vendas por categoria
# MAGIC CREATE OR REPLACE TABLE certificacao_gold.vendas_por_categoria AS
# MAGIC SELECT
# MAGIC   categoria,
# MAGIC   COUNT(*) as total_vendas,
# MAGIC   ROUND(SUM(valor), 2) as receita_total,
# MAGIC   ROUND(AVG(valor), 2) as ticket_medio,
# MAGIC   MIN(valor) as menor_venda,
# MAGIC   MAX(valor) as maior_venda
# MAGIC FROM certificacao_silver.vendas_clean
# MAGIC GROUP BY categoria
# MAGIC ORDER BY receita_total DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM certificacao_gold.vendas_por_categoria;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Gold 2: Vendas por estado e mês
# MAGIC CREATE OR REPLACE TABLE certificacao_gold.vendas_por_estado_mes AS
# MAGIC SELECT
# MAGIC   estado,
# MAGIC   YEAR(data_venda) as ano,
# MAGIC   MONTH(data_venda) as mes,
# MAGIC   COUNT(*) as total_vendas,
# MAGIC   ROUND(SUM(valor), 2) as receita
# MAGIC FROM certificacao_silver.vendas_clean
# MAGIC GROUP BY estado, YEAR(data_venda), MONTH(data_venda)
# MAGIC ORDER BY estado, ano, mes;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM certificacao_gold.vendas_por_estado_mes;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercícios
# MAGIC
# MAGIC ### Exercício A
# MAGIC Adicione mais uma tabela Gold: `vendas_por_mes` com total de vendas e receita por mês.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Seu código aqui
# MAGIC -- CREATE OR REPLACE TABLE certificacao_gold.vendas_por_mes AS ...

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício B
# MAGIC Adicione uma regra de limpeza na Silver: normalizar estados para uppercase.

# COMMAND ----------

# TODO: Seu código aqui

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício C — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Em uma arquitetura Medallion, onde devem ser aplicadas as regras de qualidade e limpeza de dados?
# MAGIC
# MAGIC - a) Bronze
# MAGIC - b) Silver
# MAGIC - c) Gold
# MAGIC - d) Em todas as camadas igualmente
# MAGIC
# MAGIC > _Sua resposta: ___

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpoint
# MAGIC
# MAGIC - [ ] Bronze: dados brutos ingeridos sem transformação
# MAGIC - [ ] Silver: dados limpos, tipados, sem duplicatas e sem nulos
# MAGIC - [ ] Gold: métricas agregadas por categoria e por estado/mês
# MAGIC - [ ] Exercícios A, B e C completados
