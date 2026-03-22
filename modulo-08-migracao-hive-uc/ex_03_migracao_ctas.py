# Databricks notebook source

# MAGIC %md
# MAGIC # Exercício 3 — Migração com CTAS (Tabelas Não-Delta)
# MAGIC
# MAGIC **Módulo 8 — Migração Hive → Unity Catalog**
# MAGIC
# MAGIC **Objetivo:** Migrar tabelas Parquet/CSV/JSON para Delta no Unity Catalog.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quando usar CTAS?
# MAGIC
# MAGIC - Tabela de origem **NÃO é Delta** (Parquet, CSV, JSON, ORC)
# MAGIC - DEEP CLONE só funciona com tabelas Delta
# MAGIC - CTAS converte para Delta automaticamente

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 — Migrar tabela Parquet para Delta no UC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Migrar tabela Parquet → Delta no Unity Catalog
# MAGIC CREATE TABLE IF NOT EXISTS empresa_migrada.bronze.vendas_ex_parquet
# MAGIC USING DELTA
# MAGIC AS SELECT * FROM hive_metastore.estudos_legado.vendas_parquet;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verificar que agora é Delta
# MAGIC DESCRIBE DETAIL empresa_migrada.bronze.vendas_ex_parquet;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 — Migração em lote (tabelas não-Delta)

# COMMAND ----------

tabelas_nao_delta = [
    ("estudos_legado", "vendas_parquet", "bronze"),
    # Adicione tabelas CSV, JSON, etc.
]

for db, tabela, camada in tabelas_nao_delta:
    try:
        origem = f"hive_metastore.{db}.{tabela}"
        destino = f"empresa_migrada.{camada}.{tabela}_migrada"

        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {destino}
            USING DELTA
            AS SELECT * FROM {origem}
        """)

        count_o = spark.sql(f"SELECT COUNT(*) as c FROM {origem}").collect()[0].c
        count_d = spark.sql(f"SELECT COUNT(*) as c FROM {destino}").collect()[0].c

        status = "OK" if count_o == count_d else f"DIVERGÊNCIA: {count_o} vs {count_d}"
        print(f"  {tabela}: {status} ({count_d} registros)")
    except Exception as e:
        print(f"  {tabela}: ERRO — {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercícios
# MAGIC
# MAGIC ### Exercício A
# MAGIC Crie uma tabela CSV no hive_metastore, depois migre para UC com CTAS.
# MAGIC Verifique que a tabela destino é Delta e tem os mesmos dados.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Criar tabela CSV e migrar

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício B — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Ao migrar uma tabela Parquet do hive_metastore para Unity Catalog, qual abordagem deve ser usada?
# MAGIC
# MAGIC - a) DEEP CLONE (funciona com qualquer formato)
# MAGIC - b) SHALLOW CLONE
# MAGIC - c) CREATE TABLE ... AS SELECT (CTAS), convertendo para Delta
# MAGIC - d) COPY INTO
# MAGIC
# MAGIC > _Sua resposta: ___

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpoint
# MAGIC
# MAGIC - [ ] Tabela Parquet migrada para Delta no UC com CTAS
# MAGIC - [ ] Contagem validada
# MAGIC - [ ] Formato Delta confirmado com DESCRIBE DETAIL
