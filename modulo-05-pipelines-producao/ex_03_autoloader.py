# Databricks notebook source

# MAGIC %md
# MAGIC # Exercício 3 — Auto Loader e Structured Streaming
# MAGIC
# MAGIC **Módulo 5 — Pipelines de Produção**
# MAGIC
# MAGIC **Objetivo:** Entender ingestão incremental — tema cobrado no exame.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conceitos-chave: Incremental Data Processing
# MAGIC
# MAGIC | Conceito | Descrição |
# MAGIC |---------|-----------|
# MAGIC | **Structured Streaming** | Engine de processamento contínuo do Spark |
# MAGIC | **Auto Loader** | `cloudFiles` — detecta novos arquivos automaticamente |
# MAGIC | **COPY INTO** | Comando SQL para ingestão incremental (idempotente) |
# MAGIC | **Trigger** | Controla quando o streaming processa dados |
# MAGIC | **Checkpoint** | Armazena o progresso do streaming |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 — Auto Loader (cloudFiles)

# COMMAND ----------

# Auto Loader — detecta novos arquivos automaticamente
# Usa checkpoint para rastrear quais arquivos já foram processados

# df_stream = (
#     spark.readStream.format("cloudFiles")
#     .option("cloudFiles.format", "csv")
#     .option("cloudFiles.inferColumnTypes", "true")
#     .option("cloudFiles.schemaLocation", "/tmp/autoloader/schema/vendas")
#     .option("header", "true")
#     .load("/data/vendas/incoming/")
# )
#
# df_stream.writeStream \
#     .format("delta") \
#     .option("checkpointLocation", "/tmp/autoloader/checkpoint/vendas") \
#     .trigger(availableNow=True) \
#     .outputMode("append") \
#     .toTable("certificacao_bronze.vendas_streaming")

print("Auto Loader: código de referência (descomente quando tiver dados)")
print()
print("Os CSVs para este exercício foram gerados pelo Faker em:")
print("  /tmp/autoloader_vendas/lote_01/")
print("  /tmp/autoloader_vendas/lote_02/")
print("  /tmp/autoloader_vendas/lote_03/")
print()
print("Substitua '/data/vendas/' por '/tmp/autoloader_vendas/' no código acima.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 — COPY INTO (alternativa SQL)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- COPY INTO usando os CSVs gerados pelo Faker
# MAGIC -- Idempotente: re-executar não duplica dados
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS certificacao_bronze.vendas_copy_into
# MAGIC USING DELTA;
# MAGIC
# MAGIC COPY INTO certificacao_bronze.vendas_copy_into
# MAGIC FROM '/tmp/autoloader_vendas/'
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true');
# MAGIC
# MAGIC SELECT COUNT(*) as total FROM certificacao_bronze.vendas_copy_into;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3 — Trigger Modes
# MAGIC
# MAGIC | Trigger | Comportamento |
# MAGIC |---------|---------------|
# MAGIC | `trigger(availableNow=True)` | Processa todos os dados disponíveis e para. Ideal para batch incremental. |
# MAGIC | `trigger(processingTime="10 seconds")` | Processa em micro-batches a cada 10 segundos. |
# MAGIC | `trigger(once=True)` | (Legado) Processa e para. Substituído por `availableNow`. |
# MAGIC | Sem trigger | Streaming contínuo — processa assim que dados chegam. |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.4 — Checkpointing
# MAGIC
# MAGIC - O checkpoint armazena o progresso do streaming
# MAGIC - Permite retomar de onde parou em caso de falha
# MAGIC - **Obrigatório** para streams que escrevem em tabelas
# MAGIC - Cada stream precisa de seu próprio checkpoint location

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercícios
# MAGIC
# MAGIC ### Exercício A — Auto Loader vs COPY INTO
# MAGIC
# MAGIC Preencha a tabela comparativa:
# MAGIC
# MAGIC | Característica | Auto Loader (cloudFiles) | COPY INTO |
# MAGIC |---------------|------------------------|-----------|
# MAGIC | Tipo | ___ | ___ |
# MAGIC | Detecção de novos arquivos | ___ | ___ |
# MAGIC | Schema inference | ___ | ___ |
# MAGIC | Performance com muitos arquivos | ___ | ___ |
# MAGIC | Linguagem | ___ | ___ |
# MAGIC | Quando usar | ___ | ___ |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício B — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Qual trigger mode processa todos os dados disponíveis e depois para o stream?
# MAGIC
# MAGIC - a) `trigger(processingTime="1 minute")`
# MAGIC - b) `trigger(continuous=True)`
# MAGIC - c) `trigger(availableNow=True)`
# MAGIC - d) `trigger(once=False)`
# MAGIC
# MAGIC > _Sua resposta: ___

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício C — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Qual é o propósito do checkpoint location em um stream?
# MAGIC
# MAGIC - a) Armazenar os dados processados em cache
# MAGIC - b) Rastrear o progresso do stream para retomar após falhas
# MAGIC - c) Definir o schema dos dados
# MAGIC - d) Limitar o número de arquivos processados
# MAGIC
# MAGIC > _Sua resposta: ___

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício D — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Auto Loader usa qual formato para detectar novos arquivos automaticamente?
# MAGIC
# MAGIC - a) `spark.readStream.format("delta")`
# MAGIC - b) `spark.readStream.format("cloudFiles")`
# MAGIC - c) `spark.readStream.format("autoloader")`
# MAGIC - d) `spark.readStream.format("incremental")`
# MAGIC
# MAGIC > _Sua resposta: ___

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpoint
# MAGIC
# MAGIC - [ ] Auto Loader (cloudFiles) entendido
# MAGIC - [ ] COPY INTO entendido
# MAGIC - [ ] Trigger modes diferenciados
# MAGIC - [ ] Checkpointing compreendido
# MAGIC - [ ] Exercícios A, B, C e D completados
