# Databricks notebook source

# MAGIC %md
# MAGIC # Exercício 2 — Formatos de Tabela: Delta vs Parquet vs CSV
# MAGIC
# MAGIC **Módulo 7 — Hive Metastore**
# MAGIC
# MAGIC **Objetivo:** Comparar formatos de armazenamento e entender trade-offs.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 — Criar tabela em formato Parquet

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS hive_metastore.estudos_legado.vendas_parquet
# MAGIC USING PARQUET
# MAGIC AS SELECT * FROM hive_metastore.estudos_legado.vendas;
# MAGIC
# MAGIC DESCRIBE DETAIL hive_metastore.estudos_legado.vendas_parquet;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 — Criar tabela em formato Delta

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS hive_metastore.estudos_legado.vendas_delta
# MAGIC USING DELTA
# MAGIC AS SELECT * FROM hive_metastore.estudos_legado.vendas;
# MAGIC
# MAGIC DESCRIBE DETAIL hive_metastore.estudos_legado.vendas_delta;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 — Comparar funcionalidades

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Delta: suporta UPDATE ✅
# MAGIC UPDATE hive_metastore.estudos_legado.vendas_delta SET valor = 3299.00 WHERE id = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Parquet: NÃO suporta UPDATE ❌
# MAGIC -- Esta query vai dar ERRO:
# MAGIC -- UPDATE hive_metastore.estudos_legado.vendas_parquet SET valor = 3299.00 WHERE id = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Delta: suporta Time Travel ✅
# MAGIC DESCRIBE HISTORY hive_metastore.estudos_legado.vendas_delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Parquet: NÃO suporta Time Travel ❌
# MAGIC -- DESCRIBE HISTORY hive_metastore.estudos_legado.vendas_parquet;  -- ERRO

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.4 — Tabela Comparativa
# MAGIC
# MAGIC | Funcionalidade | Delta | Parquet | CSV |
# MAGIC |---------------|-------|---------|-----|
# MAGIC | ACID Transactions | ✅ | ❌ | ❌ |
# MAGIC | UPDATE/DELETE/MERGE | ✅ | ❌ | ❌ |
# MAGIC | Time Travel | ✅ | ❌ | ❌ |
# MAGIC | Schema Enforcement | ✅ | ❌ | ❌ |
# MAGIC | OPTIMIZE/ZORDER | ✅ | ❌ | ❌ |
# MAGIC | Colunar (compressão) | ✅ | ✅ | ❌ |
# MAGIC | Schema embutido | ✅ | ✅ | ❌ |
# MAGIC | Leitura humana | ❌ | ❌ | ✅ |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercícios
# MAGIC
# MAGIC ### Exercício A
# MAGIC Tente executar MERGE em uma tabela Parquet. Observe e documente o erro.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Tente o MERGE e anote o erro

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício B — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Qual formato de tabela é necessário para executar operações MERGE no Databricks?
# MAGIC
# MAGIC - a) Parquet
# MAGIC - b) CSV
# MAGIC - c) Delta
# MAGIC - d) JSON
# MAGIC
# MAGIC > _Sua resposta: ___

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício C — Exam-Style
# MAGIC
# MAGIC **Pergunta:** O que o Delta Lake adiciona em cima do formato Parquet?
# MAGIC
# MAGIC - a) Compressão colunar
# MAGIC - b) Transaction log (garantindo ACID transactions)
# MAGIC - c) Suporte a leitura por humanos
# MAGIC - d) Menor tamanho de arquivo
# MAGIC
# MAGIC > _Sua resposta: ___

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpoint
# MAGIC
# MAGIC - [ ] Tabela Parquet criada e limitações observadas
# MAGIC - [ ] Tabela Delta criada e funcionalidades testadas
# MAGIC - [ ] Comparativo de formatos compreendido
