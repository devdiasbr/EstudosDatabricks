# Databricks notebook source

# MAGIC %md
# MAGIC # Exercício 5 — DEEP CLONE, SHALLOW CLONE e CTAS
# MAGIC
# MAGIC **Módulo 4 — Delta Lake**
# MAGIC
# MAGIC **Objetivo:** Copiar tabelas para backup, testes e migração.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.1 — DEEP CLONE
# MAGIC
# MAGIC Cópia completa: dados + metadados. Tabela independente da original.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS certificacao_silver.produtos_backup
# MAGIC DEEP CLONE certificacao_bronze.produtos;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) as total FROM certificacao_silver.produtos_backup;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.2 — SHALLOW CLONE
# MAGIC
# MAGIC Cópia apenas dos metadados (transaction log). Dados são referenciados da tabela original.
# MAGIC Ideal para testes e desenvolvimento (mais rápido, menos storage).

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS certificacao_silver.produtos_dev
# MAGIC SHALLOW CLONE certificacao_bronze.produtos;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Modificações no clone NÃO afetam a original
# MAGIC INSERT INTO certificacao_silver.produtos_dev VALUES
# MAGIC   (99, 'Produto Teste', 'teste', 1.00, 1, true, null, null);
# MAGIC
# MAGIC SELECT COUNT(*) as clone FROM certificacao_silver.produtos_dev;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Original inalterada
# MAGIC SELECT COUNT(*) as original FROM certificacao_bronze.produtos;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.3 — CTAS (CREATE TABLE AS SELECT)
# MAGIC
# MAGIC Cria nova tabela a partir de uma query. Útil para transformar e materializar.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE certificacao_gold.produtos_ativos AS
# MAGIC SELECT id, nome, categoria, preco
# MAGIC FROM certificacao_bronze.produtos
# MAGIC WHERE ativo = true
# MAGIC ORDER BY preco DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM certificacao_gold.produtos_ativos;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.4 — Managed Table vs External Table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Managed Table (Databricks gerencia dados + metadados)
# MAGIC -- DROP TABLE deleta TUDO
# MAGIC CREATE TABLE certificacao_bronze.managed_example (id INT, nome STRING) USING DELTA;
# MAGIC
# MAGIC DESCRIBE DETAIL certificacao_bronze.managed_example;
# MAGIC -- Observe a "location": é gerenciada pelo Databricks

# COMMAND ----------

# MAGIC %sql
# MAGIC -- External Table (você gerencia o local dos dados)
# MAGIC -- DROP TABLE deleta apenas metadados, dados permanecem no storage
# MAGIC -- CREATE TABLE certificacao_bronze.external_example (id INT, nome STRING)
# MAGIC -- USING DELTA LOCATION '/mnt/datalake/external_example';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercícios
# MAGIC
# MAGIC ### Exercício A
# MAGIC Crie um DEEP CLONE da tabela `certificacao_silver.vendas_clean` (do módulo 3).
# MAGIC Modifique dados no clone e verifique que a original não foi afetada.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Seu código aqui

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício B — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Qual a diferença entre DEEP CLONE e SHALLOW CLONE?
# MAGIC
# MAGIC - a) DEEP CLONE é mais rápido e usa menos storage
# MAGIC - b) SHALLOW CLONE copia dados e metadados, DEEP CLONE copia apenas metadados
# MAGIC - c) DEEP CLONE copia dados e metadados (independente), SHALLOW CLONE referencia dados da original
# MAGIC - d) Não há diferença funcional
# MAGIC
# MAGIC > _Sua resposta: ___

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício C — Exam-Style
# MAGIC
# MAGIC **Pergunta:** O que acontece com os dados ao executar DROP TABLE em uma Managed Table vs External Table?
# MAGIC
# MAGIC - a) Em ambos os casos, dados e metadados são deletados
# MAGIC - b) Managed: deleta dados + metadados. External: deleta apenas metadados
# MAGIC - c) Em ambos os casos, apenas metadados são deletados
# MAGIC - d) Managed: deleta apenas metadados. External: deleta dados + metadados
# MAGIC
# MAGIC > _Sua resposta: ___

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpoint
# MAGIC
# MAGIC - [ ] DEEP CLONE criado — cópia independente
# MAGIC - [ ] SHALLOW CLONE criado — referência a dados da original
# MAGIC - [ ] CTAS executado com transformação
# MAGIC - [ ] Managed vs External Table compreendido
# MAGIC - [ ] Exercícios A, B e C completados
