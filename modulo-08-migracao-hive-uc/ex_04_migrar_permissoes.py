# Databricks notebook source

# MAGIC %md
# MAGIC # Exercício 4 — Migrar Permissões para Unity Catalog
# MAGIC
# MAGIC **Módulo 8 — Migração Hive → Unity Catalog**
# MAGIC
# MAGIC **Objetivo:** Recriar permissões do Table ACLs (hive) no Unity Catalog (GRANT/REVOKE).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table ACLs (Hive — Legado) vs Unity Catalog
# MAGIC
# MAGIC | Aspecto | Table ACLs (Hive) | Unity Catalog |
# MAGIC |---------|-------------------|---------------|
# MAGIC | Granularidade | Database → Table | Catalog → Schema → Table |
# MAGIC | Comandos | GRANT/REVOKE (limitado) | GRANT/REVOKE (completo) |
# MAGIC | Grupos | Workspace groups | Account groups |
# MAGIC | Herança | Limitada | Hierárquica completa |
# MAGIC | Row/Column security | Não nativo | Dynamic Views |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 — Mapear permissões atuais

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Listar permissões existentes no hive_metastore
# MAGIC -- SHOW GRANTS ON DATABASE hive_metastore.estudos_legado;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 — Recriar permissões no Unity Catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Permissões para Data Engineers (acesso total)
# MAGIC GRANT USAGE ON CATALOG empresa_migrada TO `data_engineers`;
# MAGIC GRANT ALL PRIVILEGES ON SCHEMA empresa_migrada.bronze TO `data_engineers`;
# MAGIC GRANT ALL PRIVILEGES ON SCHEMA empresa_migrada.silver TO `data_engineers`;
# MAGIC GRANT ALL PRIVILEGES ON SCHEMA empresa_migrada.gold TO `data_engineers`;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Permissões para Analistas (somente leitura em gold)
# MAGIC GRANT USAGE ON CATALOG empresa_migrada TO `analysts`;
# MAGIC GRANT USAGE ON SCHEMA empresa_migrada.gold TO `analysts`;
# MAGIC GRANT SELECT ON SCHEMA empresa_migrada.gold TO `analysts`;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verificar
# MAGIC SHOW GRANTS ON CATALOG empresa_migrada;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercícios
# MAGIC
# MAGIC ### Exercício A
# MAGIC Crie um script Python que:
# MAGIC 1. Lê as permissões do hive_metastore
# MAGIC 2. Mapeia cada permissão para o equivalente no Unity Catalog
# MAGIC 3. Gera os comandos GRANT necessários

# COMMAND ----------

# TODO: Seu código aqui

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício B — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Durante a migração, como garantir que jobs existentes continuem funcionando enquanto as tabelas são migradas?
# MAGIC
# MAGIC - a) Migrar tudo de uma vez e atualizar todos os jobs simultaneamente
# MAGIC - b) Usar um período de coexistência onde ambos hive_metastore e UC são acessíveis
# MAGIC - c) Deletar as tabelas do hive_metastore imediatamente após CLONE
# MAGIC - d) Não é possível manter compatibilidade
# MAGIC
# MAGIC > _Sua resposta: ___

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpoint
# MAGIC
# MAGIC - [ ] Permissões atuais mapeadas
# MAGIC - [ ] Permissões recriadas no Unity Catalog
# MAGIC - [ ] Verificação com SHOW GRANTS
