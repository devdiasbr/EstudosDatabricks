# Databricks notebook source

# MAGIC %md
# MAGIC # Exercício 2 — Time Travel e Versionamento
# MAGIC
# MAGIC **Módulo 4 — Delta Lake**
# MAGIC
# MAGIC **Objetivo:** Usar Time Travel para consultar, comparar e restaurar versões anteriores.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 — DESCRIBE HISTORY

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Ver histórico de operações na tabela
# MAGIC DESCRIBE HISTORY certificacao_bronze.produtos;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 — Consultar versão específica

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Versão 0 (estado original após o CREATE)
# MAGIC SELECT * FROM certificacao_bronze.produtos VERSION AS OF 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Versão 1 (após primeiro INSERT)
# MAGIC SELECT * FROM certificacao_bronze.produtos VERSION AS OF 1;

# COMMAND ----------

# PySpark — Time Travel
df_v0 = spark.read.format("delta").option("versionAsOf", 0).table("certificacao_bronze.produtos")
df_v1 = spark.read.format("delta").option("versionAsOf", 1).table("certificacao_bronze.produtos")

print(f"Versão 0: {df_v0.count()} registros")
print(f"Versão 1: {df_v1.count()} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 — Consultar por Timestamp

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Consultar estado da tabela em um momento específico
# MAGIC -- (ajuste o timestamp para um valor válido do seu DESCRIBE HISTORY)
# MAGIC -- SELECT * FROM certificacao_bronze.produtos TIMESTAMP AS OF '2024-01-15T10:00:00';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.4 — Comparar versões

# COMMAND ----------

# Comparar registros entre versões
df_antes = spark.read.format("delta").option("versionAsOf", 1).table("certificacao_bronze.produtos")
df_depois = spark.table("certificacao_bronze.produtos")

# Registros que foram removidos
df_removidos = df_antes.subtract(df_depois)
print("Registros removidos:")
display(df_removidos)

# Registros que foram adicionados
df_novos = df_depois.subtract(df_antes)
print("Registros novos:")
display(df_novos)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.5 — RESTORE TABLE

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Ver estado atual
# MAGIC SELECT COUNT(*) as total_atual FROM certificacao_bronze.produtos;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Restaurar para versão anterior
# MAGIC RESTORE TABLE certificacao_bronze.produtos TO VERSION AS OF 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verificar que restaurou
# MAGIC SELECT COUNT(*) as total_restaurado FROM certificacao_bronze.produtos;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DESCRIBE HISTORY mostra a operação RESTORE
# MAGIC DESCRIBE HISTORY certificacao_bronze.produtos;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.6 — DESCRIBE DETAIL

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Metadados detalhados da tabela
# MAGIC DESCRIBE DETAIL certificacao_bronze.produtos;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercícios
# MAGIC
# MAGIC ### Exercício A
# MAGIC Faça 3 operações na tabela `produtos` (insert, update, delete).
# MAGIC Depois use DESCRIBE HISTORY para ver todas as operações.
# MAGIC Consulte a tabela na versão 1 e compare com a versão atual.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Seu código aqui

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício B — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Após executar `VACUUM` com retenção de 7 dias, ainda é possível fazer Time Travel para uma versão de 10 dias atrás?
# MAGIC
# MAGIC - a) Sim, VACUUM não afeta Time Travel
# MAGIC - b) Não, VACUUM remove os arquivos de dados antigos necessários para Time Travel
# MAGIC - c) Sim, se o transaction log ainda existir
# MAGIC - d) Depende do número de versões
# MAGIC
# MAGIC > _Sua resposta: ___

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício C — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Qual comando restaura uma tabela Delta para uma versão anterior?
# MAGIC
# MAGIC - a) `ROLLBACK TABLE t TO VERSION 1`
# MAGIC - b) `RESTORE TABLE t TO VERSION AS OF 1`
# MAGIC - c) `ALTER TABLE t REVERT TO VERSION 1`
# MAGIC - d) `UNDO TABLE t VERSION 1`
# MAGIC
# MAGIC > _Sua resposta: ___

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpoint
# MAGIC
# MAGIC - [ ] DESCRIBE HISTORY executado e compreendido
# MAGIC - [ ] Time Travel por versão (VERSION AS OF)
# MAGIC - [ ] Time Travel por timestamp (TIMESTAMP AS OF)
# MAGIC - [ ] Comparação entre versões realizada
# MAGIC - [ ] RESTORE TABLE executado
# MAGIC - [ ] DESCRIBE DETAIL explorado
