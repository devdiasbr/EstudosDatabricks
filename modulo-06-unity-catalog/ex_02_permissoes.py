# Databricks notebook source

# MAGIC %md
# MAGIC # Exercício 2 — Controle de Acesso: GRANT e REVOKE
# MAGIC
# MAGIC **Módulo 6 — Unity Catalog**
# MAGIC
# MAGIC **Objetivo:** Dominar permissões no Unity Catalog — cobrado no exame.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Modelo de Permissões do Unity Catalog
# MAGIC
# MAGIC | Permissão | Descrição | Aplica-se a |
# MAGIC |-----------|-----------|-------------|
# MAGIC | `USAGE` | Permite acessar o objeto (não dá acesso aos filhos) | Catalog, Schema |
# MAGIC | `SELECT` | Permite ler dados | Table, View |
# MAGIC | `MODIFY` | Permite INSERT, UPDATE, DELETE | Table |
# MAGIC | `CREATE` | Permite criar objetos dentro | Catalog, Schema |
# MAGIC | `ALL PRIVILEGES` | Todas as permissões acima | Qualquer |
# MAGIC
# MAGIC **Regra importante:** Permissões são **herdadas hierarquicamente**.
# MAGIC Para SELECT em `catalog.schema.table`, o usuário precisa de:
# MAGIC 1. USAGE no catalog
# MAGIC 2. USAGE no schema
# MAGIC 3. SELECT na table

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 — Conceder Permissões (GRANT)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Dar acesso de leitura ao grupo de analistas
# MAGIC GRANT USAGE ON CATALOG dev_certificacao TO `analysts`;
# MAGIC GRANT USAGE ON SCHEMA dev_certificacao.gold TO `analysts`;
# MAGIC GRANT SELECT ON SCHEMA dev_certificacao.gold TO `analysts`;
# MAGIC
# MAGIC -- Dar acesso completo ao grupo de engenheiros
# MAGIC GRANT USAGE ON CATALOG dev_certificacao TO `data_engineers`;
# MAGIC GRANT ALL PRIVILEGES ON SCHEMA dev_certificacao.bronze TO `data_engineers`;
# MAGIC GRANT ALL PRIVILEGES ON SCHEMA dev_certificacao.silver TO `data_engineers`;
# MAGIC GRANT ALL PRIVILEGES ON SCHEMA dev_certificacao.gold TO `data_engineers`;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 — Verificar Permissões (SHOW GRANTS)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Ver permissões em um schema
# MAGIC SHOW GRANTS ON SCHEMA dev_certificacao.bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Ver permissões de um grupo específico
# MAGIC SHOW GRANTS `analysts` ON CATALOG dev_certificacao;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 — Revogar Permissões (REVOKE)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Remover acesso
# MAGIC REVOKE SELECT ON SCHEMA dev_certificacao.gold FROM `analysts`;
# MAGIC
# MAGIC -- Verificar
# MAGIC SHOW GRANTS `analysts` ON SCHEMA dev_certificacao.gold;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.4 — Ownership

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Transferir ownership de uma tabela
# MAGIC -- ALTER TABLE dev_certificacao.bronze.eventos SET OWNER TO `data_engineers`;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercícios
# MAGIC
# MAGIC ### Exercício A — Cenário de Permissões
# MAGIC
# MAGIC Configure as permissões para o seguinte cenário:
# MAGIC - Grupo `estagiarios`: só pode LER tabelas da camada gold
# MAGIC - Grupo `data_engineers`: acesso total a todas as camadas
# MAGIC - Grupo `gestores`: pode ler gold e silver, mas não bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Seu código aqui

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício B — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Um analista precisa ler dados da tabela `prod.gold.revenue`. Quais permissões MÍNIMAS são necessárias?
# MAGIC
# MAGIC - a) SELECT na tabela `revenue`
# MAGIC - b) USAGE no catalog `prod` + SELECT na tabela `revenue`
# MAGIC - c) USAGE no catalog `prod` + USAGE no schema `gold` + SELECT na tabela `revenue`
# MAGIC - d) ALL PRIVILEGES no catalog `prod`
# MAGIC
# MAGIC > _Sua resposta: ___

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício C — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Qual a diferença entre USAGE e SELECT no Unity Catalog?
# MAGIC
# MAGIC - a) USAGE permite ler dados, SELECT permite navegar
# MAGIC - b) USAGE permite navegar/acessar o objeto, SELECT permite ler dados das tabelas
# MAGIC - c) São sinônimos
# MAGIC - d) USAGE aplica-se a tabelas, SELECT aplica-se a schemas
# MAGIC
# MAGIC > _Sua resposta: ___

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpoint
# MAGIC
# MAGIC - [ ] GRANT executado em catalog, schema e table
# MAGIC - [ ] SHOW GRANTS usado para verificar
# MAGIC - [ ] REVOKE executado
# MAGIC - [ ] Modelo de permissão hierárquica compreendido (USAGE + SELECT)
# MAGIC - [ ] Exercícios A, B e C completados
