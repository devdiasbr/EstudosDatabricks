# Databricks notebook source

# MAGIC %md
# MAGIC # Exercício 3 — Dynamic Views (Mascaramento de Dados)
# MAGIC
# MAGIC **Módulo 6 — Unity Catalog**
# MAGIC
# MAGIC **Objetivo:** Criar views que filtram ou mascaram dados com base no usuário.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 — Mascaramento de Coluna com current_user()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View sobre funcionarios do Faker — mascara CPF e salário para não-RH
# MAGIC -- Pré-requisito: execute utils/gerar_dados_faker.py
# MAGIC CREATE OR REPLACE VIEW dev_certificacao.gold.funcionarios_seguros AS
# MAGIC SELECT
# MAGIC   funcionario_id,
# MAGIC   nome,
# MAGIC   departamento,
# MAGIC   cargo,
# MAGIC   nivel,
# MAGIC   CASE
# MAGIC     WHEN is_account_group_member('rh') THEN cpf
# MAGIC     ELSE '***.***.***-**'
# MAGIC   END AS cpf,
# MAGIC   CASE
# MAGIC     WHEN is_account_group_member('rh') OR is_account_group_member('gestores')
# MAGIC       THEN salario
# MAGIC     ELSE NULL
# MAGIC   END AS salario,
# MAGIC   estado
# MAGIC FROM hive_metastore.certificacao_bronze.funcionarios;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Testar a view (resultado depende do seu grupo)
# MAGIC SELECT * FROM dev_certificacao.gold.eventos_seguros;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 — Filtro de Linha (Row-Level Security)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View que só mostra dados do próprio usuário
# MAGIC CREATE OR REPLACE VIEW dev_certificacao.gold.meus_eventos AS
# MAGIC SELECT *
# MAGIC FROM dev_certificacao.bronze.eventos
# MAGIC WHERE usuario = current_user()
# MAGIC    OR is_account_group_member('admin');

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3 — Funções Úteis para Dynamic Views
# MAGIC
# MAGIC | Função | Retorna |
# MAGIC |--------|---------|
# MAGIC | `current_user()` | Email do usuário logado |
# MAGIC | `is_account_group_member('grupo')` | Boolean — se o usuário pertence ao grupo |

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Ver quem é o usuário atual
# MAGIC SELECT current_user() as usuario_atual;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercícios
# MAGIC
# MAGIC ### Exercício A
# MAGIC Crie uma tabela `dev_certificacao.bronze.funcionarios` com colunas (id, nome, salario, departamento, cpf).
# MAGIC Crie uma Dynamic View que:
# MAGIC - Mostra salário e CPF apenas para membros do grupo `rh`
# MAGIC - Para outros, mascara salário com `***` e CPF com `***.***.***-**`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Seu código aqui

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício B — Exam-Style
# MAGIC
# MAGIC **Pergunta:** O que são Dynamic Views no Unity Catalog?
# MAGIC
# MAGIC - a) Views que se atualizam automaticamente quando dados mudam
# MAGIC - b) Views que aplicam controle de acesso em nível de coluna ou linha baseado na identidade do usuário
# MAGIC - c) Views que são criadas dinamicamente por pipelines
# MAGIC - d) Views materializadas que são atualizadas periodicamente
# MAGIC
# MAGIC > _Sua resposta: ___

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício C — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Qual função retorna TRUE se o usuário atual pertence a um grupo específico?
# MAGIC
# MAGIC - a) `current_user()`
# MAGIC - b) `has_group('grupo')`
# MAGIC - c) `is_account_group_member('grupo')`
# MAGIC - d) `user_in_group('grupo')`
# MAGIC
# MAGIC > _Sua resposta: ___

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpoint
# MAGIC
# MAGIC - [ ] Dynamic View com mascaramento de coluna criada
# MAGIC - [ ] Dynamic View com filtro de linha criada
# MAGIC - [ ] current_user() e is_account_group_member() compreendidos
# MAGIC - [ ] Exercícios A, B e C completados
