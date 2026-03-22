# Databricks notebook source

# MAGIC %md
# MAGIC # Exercício 2 — Migração com DEEP CLONE
# MAGIC
# MAGIC **Módulo 8 — Migração Hive → Unity Catalog**
# MAGIC
# MAGIC **Objetivo:** Migrar tabelas Delta do hive_metastore para Unity Catalog usando DEEP CLONE.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quando usar DEEP CLONE?
# MAGIC
# MAGIC - Tabela de **origem é Delta**
# MAGIC - Cria cópia **independente** (dados + metadados)
# MAGIC - Tabela destino não depende mais da origem
# MAGIC - Ideal para migração definitiva

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 — Criar estrutura de destino no Unity Catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Criar catalog e schemas de destino
# MAGIC CREATE CATALOG IF NOT EXISTS empresa_migrada;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS empresa_migrada.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS empresa_migrada.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS empresa_migrada.gold;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 — Migrar tabela com DEEP CLONE

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Migrar tabela Delta do hive_metastore para Unity Catalog
# MAGIC CREATE TABLE IF NOT EXISTS empresa_migrada.bronze.vendas
# MAGIC DEEP CLONE hive_metastore.estudos_legado.vendas;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verificar migração
# MAGIC SELECT 'origem' as fonte, COUNT(*) as registros FROM hive_metastore.estudos_legado.vendas
# MAGIC UNION ALL
# MAGIC SELECT 'destino', COUNT(*) FROM empresa_migrada.bronze.vendas;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 — Migração em lote (múltiplas tabelas)

# COMMAND ----------

# Script para migrar múltiplas tabelas Delta
tabelas_para_migrar = [
    ("estudos_legado", "vendas", "bronze"),
    ("estudos_legado", "vendas_delta", "bronze"),
    # Adicione mais tabelas conforme necessário
]

resultados = []
for db, tabela, camada_destino in tabelas_para_migrar:
    try:
        origem = f"hive_metastore.{db}.{tabela}"
        destino = f"empresa_migrada.{camada_destino}.{tabela}"

        # Verificar se é Delta
        detail = spark.sql(f"DESCRIBE DETAIL {origem}").collect()[0]
        if detail.format == "delta":
            spark.sql(f"CREATE TABLE IF NOT EXISTS {destino} DEEP CLONE {origem}")
            count_dest = spark.sql(f"SELECT COUNT(*) as c FROM {destino}").collect()[0].c
            resultados.append({"tabela": tabela, "metodo": "DEEP CLONE", "status": "OK", "registros": count_dest})
        else:
            resultados.append({"tabela": tabela, "metodo": "N/A", "status": "NÃO É DELTA", "registros": 0})
    except Exception as e:
        resultados.append({"tabela": tabela, "metodo": "DEEP CLONE", "status": f"ERRO: {str(e)[:100]}", "registros": 0})

if resultados:
    display(spark.createDataFrame(resultados))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercícios
# MAGIC
# MAGIC ### Exercício A
# MAGIC Migre todas as tabelas Delta do database `estudos_legado` para `empresa_migrada.bronze` usando o script em lote.

# COMMAND ----------

# TODO: Ajuste e execute o script acima

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício B — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Qual comando copia dados e metadados de uma tabela Delta para outra localização, criando uma tabela independente?
# MAGIC
# MAGIC - a) `CREATE TABLE ... SHALLOW CLONE`
# MAGIC - b) `CREATE TABLE ... DEEP CLONE`
# MAGIC - c) `COPY INTO`
# MAGIC - d) `INSERT INTO ... SELECT`
# MAGIC
# MAGIC > _Sua resposta: ___

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpoint
# MAGIC
# MAGIC - [ ] Estrutura de destino criada no Unity Catalog
# MAGIC - [ ] Tabela migrada com DEEP CLONE
# MAGIC - [ ] Contagem validada (origem = destino)
# MAGIC - [ ] Script de migração em lote executado
