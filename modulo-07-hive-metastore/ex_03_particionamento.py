# Databricks notebook source

# MAGIC %md
# MAGIC # Exercício 3 — Particionamento e Partition Pruning
# MAGIC
# MAGIC **Módulo 7 — Hive Metastore**
# MAGIC
# MAGIC **Objetivo:** Entender particionamento de tabelas e otimização de queries.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 — Criar tabela particionada

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS hive_metastore.estudos_legado.vendas_particionada
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (ano, mes)
# MAGIC AS
# MAGIC SELECT *,
# MAGIC   YEAR(data_venda) as ano,
# MAGIC   MONTH(data_venda) as mes
# MAGIC FROM hive_metastore.estudos_legado.vendas;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Ver partições
# MAGIC SHOW PARTITIONS hive_metastore.estudos_legado.vendas_particionada;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 — Partition Pruning
# MAGIC
# MAGIC Quando você filtra pela coluna de partição, o Spark lê APENAS os arquivos relevantes.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Esta query lê APENAS a partição ano=2024/mes=1
# MAGIC -- Muito mais eficiente que ler a tabela toda
# MAGIC SELECT * FROM hive_metastore.estudos_legado.vendas_particionada
# MAGIC WHERE ano = 2024 AND mes = 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3 — Verificar se partition pruning aconteceu

# COMMAND ----------

# MAGIC %sql
# MAGIC -- EXPLAIN mostra o plano de execução
# MAGIC EXPLAIN SELECT * FROM hive_metastore.estudos_legado.vendas_particionada
# MAGIC WHERE ano = 2024 AND mes = 1;
# MAGIC -- Procure por "PartitionFilters" no plano

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.4 — Quando particionar?
# MAGIC
# MAGIC | Cenário | Particionar? | Por quê? |
# MAGIC |---------|-------------|----------|
# MAGIC | Tabela com 1GB+ por partição | ✅ | Partições grandes o suficiente |
# MAGIC | Tabela pequena (<1GB total) | ❌ | Over-partitioning piora performance |
# MAGIC | Filtros frequentes por data | ✅ | Partition pruning acelera queries |
# MAGIC | Alta cardinalidade (ex: user_id) | ❌ | Muitos arquivos pequenos |
# MAGIC | Prefira ZORDER quando... | ✅ | Múltiplas colunas de filtro sem hierarquia |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercícios
# MAGIC
# MAGIC ### Exercício A
# MAGIC Crie uma tabela particionada por `estado`. Insira dados e consulte filtrando por estado.
# MAGIC Use EXPLAIN para verificar se partition pruning aconteceu.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Seu código aqui

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício B — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Quando é recomendado particionar uma tabela Delta?
# MAGIC
# MAGIC - a) Sempre que possível para melhor performance
# MAGIC - b) Quando cada partição tem pelo menos 1GB de dados
# MAGIC - c) Quando a tabela tem menos de 100 registros
# MAGIC - d) Quando a coluna de partição tem alta cardinalidade
# MAGIC
# MAGIC > _Sua resposta: ___

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício C — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Qual a alternativa ao particionamento para acelerar filtros em tabelas Delta sem criar partições físicas?
# MAGIC
# MAGIC - a) Indexação
# MAGIC - b) Z-Ordering (OPTIMIZE ZORDER BY)
# MAGIC - c) Bucketing
# MAGIC - d) Caching
# MAGIC
# MAGIC > _Sua resposta: ___

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpoint
# MAGIC
# MAGIC - [ ] Tabela particionada criada
# MAGIC - [ ] Partition pruning verificado com EXPLAIN
# MAGIC - [ ] Critérios para particionar compreendidos
# MAGIC - [ ] Exercícios A, B e C completados
