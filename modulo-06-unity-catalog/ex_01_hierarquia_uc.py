# Databricks notebook source

# MAGIC %md
# MAGIC # Exercício 1 — Hierarquia do Unity Catalog
# MAGIC
# MAGIC **Módulo 6 — Unity Catalog & Governança**
# MAGIC
# MAGIC **Vídeo de apoio:** [Governança de Dados](https://www.youtube.com/playlist?list=PLnxUgOpvXnWzTkha0174lXfES019PQz9U) — Vídeo 12
# MAGIC
# MAGIC **Objetivo:** Criar e navegar na hierarquia completa do Unity Catalog.
# MAGIC
# MAGIC > **NOTA:** Unity Catalog requer Databricks com UC habilitado (não disponível no Community Edition).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hierarquia do Unity Catalog
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────┐
# MAGIC │              METASTORE                   │  ← Nível da conta (1 por região)
# MAGIC │  ┌──────────────────────────────────┐    │
# MAGIC │  │          CATALOG                  │   │  ← Agrupamento lógico (ex: prod, dev)
# MAGIC │  │  ┌───────────────────────────┐    │   │
# MAGIC │  │  │        SCHEMA              │   │   │  ← Database (ex: bronze, silver, gold)
# MAGIC │  │  │  ┌────────────────────┐    │   │   │
# MAGIC │  │  │  │  TABLE / VIEW      │    │   │   │  ← Dados
# MAGIC │  │  │  │  FUNCTION          │    │   │   │  ← UDFs
# MAGIC │  │  │  └────────────────────┘    │   │   │
# MAGIC │  │  └───────────────────────────┘    │   │
# MAGIC │  └──────────────────────────────────┘    │
# MAGIC └─────────────────────────────────────────┘
# MAGIC
# MAGIC Referência: catalog.schema.table
# MAGIC Exemplo:    prod.gold.vendas_diarias
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 — Explorar Metastore

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Listar todos os catalogs disponíveis
# MAGIC SHOW CATALOGS;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 — Criar Catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Criar catalog para ambiente de desenvolvimento
# MAGIC CREATE CATALOG IF NOT EXISTS dev_certificacao
# MAGIC COMMENT 'Catalog para estudos da certificação Databricks';
# MAGIC
# MAGIC -- Criar catalog para produção (simulado)
# MAGIC CREATE CATALOG IF NOT EXISTS prod_certificacao
# MAGIC COMMENT 'Catalog simulando ambiente de produção';

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CATALOGS;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.3 — Criar Schemas (Databases)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG dev_certificacao;
# MAGIC
# MAGIC -- Criar schemas por camada (Medallion)
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze COMMENT 'Dados brutos';
# MAGIC CREATE SCHEMA IF NOT EXISTS silver COMMENT 'Dados limpos e validados';
# MAGIC CREATE SCHEMA IF NOT EXISTS gold COMMENT 'Dados agregados para consumo';
# MAGIC
# MAGIC SHOW SCHEMAS;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.4 — Criar Tabelas com Referência Completa

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sempre use referência de 3 níveis: catalog.schema.table
# MAGIC CREATE TABLE IF NOT EXISTS dev_certificacao.bronze.eventos (
# MAGIC   id BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   evento STRING NOT NULL,
# MAGIC   usuario STRING,
# MAGIC   timestamp TIMESTAMP DEFAULT current_timestamp(),
# MAGIC   payload STRING
# MAGIC ) COMMENT 'Eventos brutos do sistema';
# MAGIC
# MAGIC INSERT INTO dev_certificacao.bronze.eventos (evento, usuario, payload)
# MAGIC VALUES
# MAGIC   ('login', 'ana@empresa.com', '{"ip": "192.168.1.1"}'),
# MAGIC   ('compra', 'bruno@empresa.com', '{"produto": "notebook", "valor": 3500}'),
# MAGIC   ('logout', 'ana@empresa.com', '{"sessao": "abc123"}');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dev_certificacao.bronze.eventos;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.5 — Navegar pela Hierarquia

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Ver detalhes de um catalog
# MAGIC DESCRIBE CATALOG dev_certificacao;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Ver detalhes de um schema
# MAGIC DESCRIBE SCHEMA dev_certificacao.bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Listar tabelas em um schema
# MAGIC SHOW TABLES IN dev_certificacao.bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Detalhes de uma tabela
# MAGIC DESCRIBE DETAIL dev_certificacao.bronze.eventos;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercícios
# MAGIC
# MAGIC ### Exercício A
# MAGIC Crie a mesma estrutura (bronze/silver/gold) no catalog `prod_certificacao`.
# MAGIC Crie uma tabela `clientes` na camada bronze e `clientes_ativos` na gold.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Seu código aqui

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício B
# MAGIC Desenhe a hierarquia completa dos seus catalogs num diagrama:
# MAGIC
# MAGIC ```
# MAGIC Metastore
# MAGIC ├── dev_certificacao
# MAGIC │   ├── bronze
# MAGIC │   │   └── ___
# MAGIC │   ├── silver
# MAGIC │   │   └── ___
# MAGIC │   └── gold
# MAGIC │       └── ___
# MAGIC └── prod_certificacao
# MAGIC     ├── ___
# MAGIC     └── ___
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício C — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Qual é a referência correta para acessar uma tabela no Unity Catalog?
# MAGIC
# MAGIC - a) `schema.table`
# MAGIC - b) `catalog.table`
# MAGIC - c) `catalog.schema.table`
# MAGIC - d) `metastore.catalog.schema.table`
# MAGIC
# MAGIC > _Sua resposta: ___

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpoint
# MAGIC
# MAGIC - [ ] Catalogs criados (dev e prod)
# MAGIC - [ ] Schemas bronze/silver/gold criados
# MAGIC - [ ] Tabelas criadas com referência de 3 níveis
# MAGIC - [ ] Navegação: SHOW CATALOGS, SCHEMAS, TABLES
# MAGIC - [ ] DESCRIBE CATALOG, SCHEMA, DETAIL explorados
