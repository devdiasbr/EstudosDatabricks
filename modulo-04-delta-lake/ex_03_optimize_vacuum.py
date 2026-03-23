# Databricks notebook source

# MAGIC %md
# MAGIC # Exercício 3 — OPTIMIZE, Z-Ordering e VACUUM
# MAGIC
# MAGIC **Módulo 4 — Delta Lake**
# MAGIC
# MAGIC **Objetivo:** Otimizar tabelas Delta para performance e gerenciar armazenamento.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 — OPTIMIZE
# MAGIC
# MAGIC Compacta small files em arquivos maiores para melhor performance de leitura.

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE certificacao_bronze.vendas;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 — OPTIMIZE + ZORDER
# MAGIC
# MAGIC Z-Ordering co-localiza dados relacionados no mesmo conjunto de arquivos, acelerando queries que filtram por essas colunas.

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE certificacao_bronze.vendas ZORDER BY (categoria);

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3 — VACUUM
# MAGIC
# MAGIC Remove arquivos antigos que não são mais referenciados pelo transaction log.
# MAGIC
# MAGIC **ATENÇÃO:** Após VACUUM, Time Travel para versões cujos arquivos foram removidos NÃO funciona mais.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Ver arquivos no diretório da tabela ANTES do vacuum
# MAGIC DESCRIBE DETAIL certificacao_bronze.vendas;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- VACUUM com retenção padrão (168 horas = 7 dias)
# MAGIC VACUUM certificacao_bronze.vendas RETAIN 168 HOURS;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DRY RUN: ver o que seria removido sem realmente remover
# MAGIC VACUUM certificacao_bronze.vendas RETAIN 168 HOURS DRY RUN;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.4 — Tentando VACUUM com retenção baixa (vai dar erro!)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Isso vai dar ERRO por segurança (retenção mínima padrão é 168h)
# MAGIC -- VACUUM certificacao_bronze.vendas RETAIN 0 HOURS;

# COMMAND ----------

# Para permitir retenção menor que 168h (NÃO FAÇA EM PRODUÇÃO):
# spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
# spark.sql("VACUUM certificacao_bronze.vendas RETAIN 0 HOURS")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercícios
# MAGIC
# MAGIC ### Exercício A
# MAGIC Crie uma tabela com muitos registros (1000+). Faça vários INSERTs pequenos para criar small files.
# MAGIC Execute OPTIMIZE e observe o resultado.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Seu código aqui

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício B — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Qual é a retenção padrão do VACUUM em horas?
# MAGIC
# MAGIC - a) 24 horas
# MAGIC - b) 72 horas
# MAGIC - c) 168 horas (7 dias)
# MAGIC - d) 720 horas (30 dias)
# MAGIC
# MAGIC > _Sua resposta: ___

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício C — Exam-Style
# MAGIC
# MAGIC **Pergunta:** O que o ZORDER faz?
# MAGIC
# MAGIC - a) Ordena os registros dentro da tabela permanentemente
# MAGIC - b) Co-localiza dados relacionados nos mesmos arquivos para acelerar filtros
# MAGIC - c) Cria um índice na coluna especificada
# MAGIC - d) Particiona a tabela pela coluna especificada
# MAGIC
# MAGIC > _Sua resposta: ___

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício D — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Após rodar `OPTIMIZE`, o que acontece com os small files originais?
# MAGIC
# MAGIC - a) São deletados imediatamente
# MAGIC - b) Ficam marcados como removidos no transaction log mas ainda existem no storage
# MAGIC - c) São movidos para uma pasta de backup
# MAGIC - d) São comprimidos em formato ZIP
# MAGIC
# MAGIC > _Sua resposta: ___

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpoint
# MAGIC
# MAGIC - [ ] OPTIMIZE executado
# MAGIC - [ ] ZORDER aplicado em coluna de filtro
# MAGIC - [ ] VACUUM executado com DRY RUN
# MAGIC - [ ] Entendida a relação VACUUM ↔ Time Travel
# MAGIC - [ ] Exercícios A, B, C e D completados
