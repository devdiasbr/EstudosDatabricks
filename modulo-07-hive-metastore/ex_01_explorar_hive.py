# Databricks notebook source

# MAGIC %md
# MAGIC # Exercício 1 — Explorar o Hive Metastore
# MAGIC
# MAGIC **Módulo 7 — Hive Metastore**
# MAGIC
# MAGIC **Objetivo:** Navegar pelo hive_metastore legado e entender sua estrutura.

# COMMAND ----------

# MAGIC %md
# MAGIC ## O que é o Hive Metastore?
# MAGIC
# MAGIC - Catálogo de metadados padrão do Apache Hive/Spark
# MAGIC - **Hierarquia:** Database (Schema) → Table → Partition
# MAGIC - **NÃO** tem nível "Catalog" (namespace único por workspace)
# MAGIC - Escopo: **workspace-level** (não compartilha entre workspaces)
# MAGIC - Armazenamento: DBFS (Databricks File System)
# MAGIC - Controle de acesso: Table ACLs (limitado)
# MAGIC
# MAGIC ### Hive Metastore vs Unity Catalog
# MAGIC
# MAGIC | Característica | Hive Metastore | Unity Catalog |
# MAGIC |---------------|----------------|---------------|
# MAGIC | Hierarquia | Database → Table | Catalog → Schema → Table |
# MAGIC | Escopo | Workspace | Account (multi-workspace) |
# MAGIC | Controle de acesso | Table ACLs (limitado) | GRANT/REVOKE granular |
# MAGIC | Data Lineage | Não nativo | Automático |
# MAGIC | Auditoria | Manual | Nativa |
# MAGIC | Storage | DBFS (workspace-level) | External Locations (account-level) |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 — Explorar o hive_metastore

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Listar databases no hive_metastore
# MAGIC SHOW DATABASES IN hive_metastore;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 — Criar database no hive_metastore

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS hive_metastore.estudos_legado
# MAGIC COMMENT 'Database legado para estudos de migração';
# MAGIC
# MAGIC DESCRIBE DATABASE hive_metastore.estudos_legado;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.3 — Criar tabela no hive_metastore

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS hive_metastore.estudos_legado.vendas (
# MAGIC   id INT,
# MAGIC   produto STRING,
# MAGIC   categoria STRING,
# MAGIC   valor DOUBLE,
# MAGIC   data_venda DATE,
# MAGIC   estado STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC INSERT INTO hive_metastore.estudos_legado.vendas VALUES
# MAGIC   (1, 'Notebook', 'eletronicos', 3500.00, '2024-01-15', 'SP'),
# MAGIC   (2, 'Mouse', 'perifericos', 89.90, '2024-01-20', 'RJ'),
# MAGIC   (3, 'Monitor', 'eletronicos', 1899.00, '2024-02-01', 'MG'),
# MAGIC   (4, 'Teclado', 'perifericos', 199.90, '2024-02-10', 'SP'),
# MAGIC   (5, 'SSD', 'armazenamento', 399.90, '2024-03-05', 'RJ');

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL hive_metastore.estudos_legado.vendas;
# MAGIC -- Observe: a location está no DBFS

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.4 — Referência de 2 níveis vs 3 níveis
# MAGIC
# MAGIC - **Hive Metastore:** `database.table` (2 níveis)
# MAGIC - **Unity Catalog:** `catalog.schema.table` (3 níveis)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Hive: referência de 2 níveis
# MAGIC SELECT * FROM hive_metastore.estudos_legado.vendas;
# MAGIC
# MAGIC -- No hive_metastore, "hive_metastore" é o catalog implícito
# MAGIC -- Muitos scripts legados usam apenas: estudos_legado.vendas

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.5 — DBFS (Databricks File System)

# COMMAND ----------

# DBFS: sistema de arquivos do workspace
display(dbutils.fs.ls("dbfs:/user/hive/warehouse/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercícios
# MAGIC
# MAGIC ### Exercício A
# MAGIC Liste todas as tabelas no database `estudos_legado`.
# MAGIC Use DESCRIBE DETAIL para ver a localização física de cada tabela.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Seu código aqui

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício B — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Por que o Hive Metastore é considerado legado? Cite 3 limitações.
# MAGIC
# MAGIC 1. ___
# MAGIC 2. ___
# MAGIC 3. ___

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício C — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Qual a principal diferença de escopo entre Hive Metastore e Unity Catalog?
# MAGIC
# MAGIC - a) Hive Metastore é por conta, Unity Catalog é por workspace
# MAGIC - b) Hive Metastore é por workspace, Unity Catalog é por conta (multi-workspace)
# MAGIC - c) Ambos são por workspace
# MAGIC - d) Ambos são por conta
# MAGIC
# MAGIC > _Sua resposta: ___

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpoint
# MAGIC
# MAGIC - [ ] hive_metastore explorado
# MAGIC - [ ] Database e tabela legada criados
# MAGIC - [ ] DESCRIBE DETAIL mostra localização no DBFS
# MAGIC - [ ] Diferenças Hive vs UC compreendidas
