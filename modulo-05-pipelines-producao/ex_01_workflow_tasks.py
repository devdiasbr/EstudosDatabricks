# Databricks notebook source

# MAGIC %md
# MAGIC # Exercício 1 — Workflows e Task Values
# MAGIC
# MAGIC **Módulo 5 — Pipelines de Produção**
# MAGIC
# MAGIC **Vídeo de apoio:** [Automação de Pipelines com Workflows](https://www.youtube.com/playlist?list=PLnxUgOpvXnWzTkha0174lXfES019PQz9U) — Vídeo 10
# MAGIC
# MAGIC **Objetivo:** Criar e entender Workflows do Databricks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conceitos-chave: Workflows
# MAGIC
# MAGIC - **Job:** Um workflow com uma ou mais tasks
# MAGIC - **Task:** Uma unidade de trabalho (notebook, script, DLT pipeline, etc.)
# MAGIC - **DAG:** Grafo de dependências entre tasks
# MAGIC - **Schedule:** Agendamento (cron expression)
# MAGIC - **Job Cluster:** Cluster criado especificamente para o job (mais barato)
# MAGIC - **Task Values:** Mecanismo para passar dados entre tasks

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 — Task 1: Ingestão (Bronze)
# MAGIC
# MAGIC Em produção, este notebook seria a Task 1 do Workflow.
# MAGIC
# MAGIC > **Pré-requisito:** Execute `utils/gerar_dados_faker.py` antes de começar.

# COMMAND ----------

# Simular ingestão incremental — lê novos registros de vendas do dia
from pyspark.sql.functions import current_date, lit
from datetime import date

# Pegar uma amostra de vendas como se fossem "dados novos do dia"
df_ingestao = (spark.table("certificacao_bronze.vendas")
               .filter("data_venda >= '2024-12-01'")
               .limit(100)
               .withColumn("ingerido_em", current_date()))
registros_ingeridos = df_ingestao.count()

# Salvar na bronze
df_ingestao.write.format("delta").mode("append").saveAsTable("certificacao_bronze.ingestao_diaria")

print(f"Ingeridos: {registros_ingeridos} registros")

# COMMAND ----------

# Passar informação para a próxima task usando Task Values
# (funciona quando executado dentro de um Workflow)
try:
    dbutils.jobs.taskValues.set(key="registros_ingeridos", value=registros_ingeridos)
    dbutils.jobs.taskValues.set(key="tabela_origem", value="certificacao_bronze.ingestao_diaria")
    print("Task values definidos com sucesso!")
except:
    print("Task values só funcionam dentro de um Workflow — OK para testes interativos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 — Task 2: Transformação (Silver)
# MAGIC
# MAGIC Este notebook seria a Task 2, dependente da Task 1.

# COMMAND ----------

# Recuperar task values da task anterior
try:
    tabela = dbutils.jobs.taskValues.get(taskKey="task_ingestao", key="tabela_origem")
except:
    tabela = "certificacao_bronze.ingestao_diaria"  # fallback para teste interativo

df_bronze = spark.table(tabela)

# Transformações
from pyspark.sql.functions import upper, current_timestamp

df_silver = df_bronze \
    .withColumn("categoria", upper("categoria")) \
    .withColumn("processado_em", current_timestamp()) \
    .filter("valor > 0")

df_silver.write.format("delta").mode("overwrite").saveAsTable("certificacao_silver.ingestao_limpa")

print(f"Silver: {df_silver.count()} registros processados")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.3 — Task 3: Agregação (Gold)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE certificacao_gold.resumo_diario AS
# MAGIC SELECT
# MAGIC   categoria,
# MAGIC   COUNT(*) as total,
# MAGIC   ROUND(SUM(valor), 2) as receita,
# MAGIC   ROUND(AVG(valor), 2) as ticket_medio,
# MAGIC   current_timestamp() as gerado_em
# MAGIC FROM certificacao_silver.ingestao_limpa
# MAGIC GROUP BY categoria;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM certificacao_gold.resumo_diario;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercícios
# MAGIC
# MAGIC ### Exercício A — Criar Workflow na UI
# MAGIC
# MAGIC Passo a passo para criar o Workflow (faça na interface do Databricks):
# MAGIC
# MAGIC 1. Vá em **Workflows** → **Create Job**
# MAGIC 2. Nome: `pipeline_vendas_diario`
# MAGIC 3. Adicione 3 tasks apontando para os 3 notebooks (ingestão, transformação, agregação)
# MAGIC 4. Configure dependências: Task 2 depende de Task 1, Task 3 depende de Task 2
# MAGIC 5. Configure cluster: **Job Cluster** com runtime LTS
# MAGIC 6. Execute manualmente e verifique os resultados
# MAGIC
# MAGIC - [ ] Workflow criado
# MAGIC - [ ] Todas as 3 tasks executadas com sucesso
# MAGIC - [ ] Verificado resultado na tabela gold

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício B — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Qual a vantagem de usar Job Clusters em vez de All-Purpose Clusters para Workflows de produção?
# MAGIC
# MAGIC - a) Job Clusters suportam mais linguagens
# MAGIC - b) Job Clusters são criados e destruídos automaticamente, reduzindo custos
# MAGIC - c) Job Clusters têm mais memória disponível
# MAGIC - d) Job Clusters permitem uso interativo
# MAGIC
# MAGIC > _Sua resposta: ___

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício C — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Como passar dados entre tasks em um Workflow?
# MAGIC
# MAGIC - a) Usando variáveis globais do Python
# MAGIC - b) Usando `dbutils.jobs.taskValues.set()` e `.get()`
# MAGIC - c) Usando `spark.conf.set()`
# MAGIC - d) Usando widgets
# MAGIC
# MAGIC > _Sua resposta: ___

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpoint
# MAGIC
# MAGIC - [ ] 3 notebooks de tasks criados (ingestão, transformação, agregação)
# MAGIC - [ ] Task values implementados
# MAGIC - [ ] Workflow criado na UI do Databricks
# MAGIC - [ ] Pipeline executado com sucesso
# MAGIC - [ ] Exercícios B e C respondidos
