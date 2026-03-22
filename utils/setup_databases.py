# Databricks notebook source

# MAGIC %md
# MAGIC # Setup Inicial — Databases para o Curso
# MAGIC
# MAGIC **Execute este notebook ANTES de começar os exercícios.**
# MAGIC
# MAGIC Cria a estrutura de databases necessária para todos os módulos.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criar databases no hive_metastore (Módulos 1–5)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Camadas Medallion
# MAGIC CREATE DATABASE IF NOT EXISTS certificacao_bronze COMMENT 'Camada bronze - dados brutos';
# MAGIC CREATE DATABASE IF NOT EXISTS certificacao_silver COMMENT 'Camada silver - dados limpos';
# MAGIC CREATE DATABASE IF NOT EXISTS certificacao_gold COMMENT 'Camada gold - dados agregados';
# MAGIC
# MAGIC -- Database para exercícios de Hive (Módulo 7)
# MAGIC CREATE DATABASE IF NOT EXISTS hive_metastore.estudos_legado COMMENT 'Database legado para estudos';
# MAGIC
# MAGIC SHOW DATABASES;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criar catalogs no Unity Catalog (Módulos 6 e 8)
# MAGIC
# MAGIC > **NOTA:** Requer Databricks com Unity Catalog habilitado.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Descomentar quando tiver UC habilitado:
# MAGIC -- CREATE CATALOG IF NOT EXISTS dev_certificacao COMMENT 'Ambiente de desenvolvimento';
# MAGIC -- CREATE CATALOG IF NOT EXISTS prod_certificacao COMMENT 'Ambiente de produção simulado';
# MAGIC -- CREATE CATALOG IF NOT EXISTS empresa_migrada COMMENT 'Destino da migração Hive→UC';
# MAGIC --
# MAGIC -- USE CATALOG dev_certificacao;
# MAGIC -- CREATE SCHEMA IF NOT EXISTS bronze;
# MAGIC -- CREATE SCHEMA IF NOT EXISTS silver;
# MAGIC -- CREATE SCHEMA IF NOT EXISTS gold;
# MAGIC --
# MAGIC -- USE CATALOG empresa_migrada;
# MAGIC -- CREATE SCHEMA IF NOT EXISTS bronze;
# MAGIC -- CREATE SCHEMA IF NOT EXISTS silver;
# MAGIC -- CREATE SCHEMA IF NOT EXISTS gold;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificar setup

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW DATABASES LIKE 'certificacao*';

# COMMAND ----------

print("Setup concluído! Databases criados com sucesso.")
print("")
print("PRÓXIMO PASSO: Execute o notebook 'gerar_dados_faker' para popular as tabelas.")
print("  Ele vai gerar:")
print("    - 5.000 clientes")
print("    - 500 produtos")
print("    - 50.000 vendas")
print("    - 20.000 eventos")
print("    - 1.000 funcionários")
print("    - Dados 'sujos' para exercícios de limpeza")
print("    - CSVs incrementais para Auto Loader")
