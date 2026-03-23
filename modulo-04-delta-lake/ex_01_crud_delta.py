# Databricks notebook source

# MAGIC %md
# MAGIC # Exercício 1 — CRUD em Delta Lake + MERGE
# MAGIC
# MAGIC **Módulo 4 — Delta Lake Profundo**
# MAGIC
# MAGIC **Vídeo de apoio:** [Delta Lake e Databases](https://www.youtube.com/playlist?list=PLnxUgOpvXnWzTkha0174lXfES019PQz9U) — Vídeo 8
# MAGIC
# MAGIC **Objetivo:** Dominar operações DML em tabelas Delta — muito cobrado no exame.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 — Tabela de trabalho para este exercício
# MAGIC
# MAGIC > **Pré-requisito:** Execute `utils/gerar_dados_faker.py` antes de começar.
# MAGIC
# MAGIC Vamos criar uma **cópia de trabalho** dos produtos para praticar DML sem alterar os dados originais.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Criar tabela de trabalho a partir dos dados Faker
# MAGIC CREATE OR REPLACE TABLE certificacao_bronze.produtos_trabalho_trabalho
# MAGIC AS SELECT produto_id AS id, nome, categoria, preco, estoque, ativo
# MAGIC    FROM certificacao_bronze.produtos_trabalho
# MAGIC    LIMIT 20;
# MAGIC
# MAGIC SELECT * FROM certificacao_bronze.produtos_trabalho_trabalho;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 — INSERT

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO certificacao_bronze.produtos_trabalho_trabalho VALUES
# MAGIC   (21, 'Novo Produto Teste', 'eletronicos', 999.00, 10, true),
# MAGIC   (22, 'Outro Produto',      'perifericos',  49.90,  5, true);
# MAGIC
# MAGIC SELECT * FROM certificacao_bronze.produtos_trabalho_trabalho ORDER BY id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.3 — UPDATE

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Atualizar preço do Notebook
# MAGIC UPDATE certificacao_bronze.produtos_trabalho
# MAGIC SET preco = 3299.00
# MAGIC WHERE id = 1;
# MAGIC
# MAGIC -- Reativar Webcam
# MAGIC UPDATE certificacao_bronze.produtos_trabalho
# MAGIC SET ativo = true
# MAGIC WHERE id = 5;
# MAGIC
# MAGIC SELECT * FROM certificacao_bronze.produtos_trabalho;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.4 — DELETE

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Deletar produto inativo (se houvesse)
# MAGIC DELETE FROM certificacao_bronze.produtos_trabalho
# MAGIC WHERE ativo = false;
# MAGIC
# MAGIC SELECT * FROM certificacao_bronze.produtos_trabalho;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.5 — MERGE (Upsert) ⚠️ Muito cobrado no exame!
# MAGIC
# MAGIC MERGE combina INSERT + UPDATE em uma única operação.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Criar tabela de atualizações (simulando dados novos)
# MAGIC CREATE OR REPLACE TEMP VIEW produtos_atualizacao AS
# MAGIC SELECT * FROM VALUES
# MAGIC   (1, 'Notebook Dell XPS', 'eletronicos', 3999.00, 45, true),    -- UPDATE: novo nome e preço
# MAGIC   (2, 'Mouse Logitech MX', 'perifericos', 99.90, 180, true),     -- UPDATE: novo nome e preço
# MAGIC   (6, 'Headset Gamer', 'perifericos', 450.00, 80, true),         -- INSERT: produto novo
# MAGIC   (7, 'SSD 1TB NVMe', 'armazenamento', 399.90, 120, true)       -- INSERT: produto novo
# MAGIC AS t(id, nome, categoria, preco, estoque, ativo);

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO certificacao_bronze.produtos_trabalho AS target
# MAGIC USING produtos_atualizacao AS source
# MAGIC ON target.id = source.id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     target.nome = source.nome,
# MAGIC     target.preco = source.preco,
# MAGIC     target.estoque = source.estoque
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (id, nome, categoria, preco, estoque, ativo)
# MAGIC   VALUES (source.id, source.nome, source.categoria, source.preco, source.estoque, source.ativo);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verificar resultado do MERGE
# MAGIC SELECT * FROM certificacao_bronze.produtos_trabalho ORDER BY id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.6 — MERGE com DELETE

# COMMAND ----------

# MAGIC %sql
# MAGIC -- MERGE que também deleta registros marcados como inativos na source
# MAGIC CREATE OR REPLACE TEMP VIEW produtos_sync AS
# MAGIC SELECT * FROM VALUES
# MAGIC   (3, 'Teclado Mecânico', 'perifericos', 179.90, 100, true),      -- UPDATE
# MAGIC   (5, 'Webcam HD', 'perifericos', 0.00, 0, false)                 -- DELETAR (ativo=false)
# MAGIC AS t(id, nome, categoria, preco, estoque, ativo);

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO certificacao_bronze.produtos_trabalho AS target
# MAGIC USING produtos_sync AS source
# MAGIC ON target.id = source.id
# MAGIC WHEN MATCHED AND source.ativo = false THEN DELETE
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM certificacao_bronze.produtos_trabalho ORDER BY id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercícios
# MAGIC
# MAGIC ### Exercício A
# MAGIC Crie uma tabela `certificacao_bronze.clientes` com colunas (id, nome, email, cidade, ativo).
# MAGIC Insira 5 registros, atualize 2, delete 1.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Seu código aqui

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício B
# MAGIC Crie uma tabela de `novos_clientes` e faça um MERGE na tabela de clientes:
# MAGIC - Se o cliente já existe (mesmo id): atualizar email e cidade
# MAGIC - Se é novo: inserir
# MAGIC - Se está inativo na source: deletar

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Seu código aqui

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício C — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Qual cláusula do MERGE é executada quando um registro existe na source mas NÃO existe na target?
# MAGIC
# MAGIC - a) WHEN MATCHED THEN UPDATE
# MAGIC - b) WHEN MATCHED THEN DELETE
# MAGIC - c) WHEN NOT MATCHED THEN INSERT
# MAGIC - d) WHEN NOT MATCHED BY SOURCE THEN DELETE
# MAGIC
# MAGIC > _Sua resposta: ___

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício D — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Qual operação DML NÃO é suportada em tabelas Parquet, mas é suportada em tabelas Delta?
# MAGIC
# MAGIC - a) SELECT
# MAGIC - b) INSERT
# MAGIC - c) UPDATE
# MAGIC - d) CREATE TABLE
# MAGIC
# MAGIC > _Sua resposta: ___

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpoint
# MAGIC
# MAGIC - [ ] CREATE TABLE Delta executado
# MAGIC - [ ] INSERT, UPDATE, DELETE praticados
# MAGIC - [ ] MERGE com UPDATE + INSERT executado
# MAGIC - [ ] MERGE com DELETE condicional executado
# MAGIC - [ ] Exercícios A, B, C e D completados
