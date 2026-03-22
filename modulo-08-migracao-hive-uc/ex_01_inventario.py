# Databricks notebook source

# MAGIC %md
# MAGIC # Exercício 1 — Inventário Automatizado do Hive Metastore
# MAGIC
# MAGIC **Módulo 8 — Migração Hive → Unity Catalog**
# MAGIC
# MAGIC **Objetivo:** Gerar inventário completo de databases, tabelas e views antes da migração.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 — Checklist Pré-Migração
# MAGIC
# MAGIC Antes de migrar, é essencial mapear tudo que existe:
# MAGIC
# MAGIC - [ ] Inventário de todas as databases
# MAGIC - [ ] Inventário de todas as tabelas (formato, tamanho, localização)
# MAGIC - [ ] Mapeamento de permissões atuais (Table ACLs)
# MAGIC - [ ] Lista de external locations usadas
# MAGIC - [ ] Lista de jobs/notebooks que referenciam `hive_metastore`
# MAGIC - [ ] Plano de rollback definido

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 — Listar todas as databases

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW DATABASES IN hive_metastore;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.3 — Inventário automatizado de tabelas

# COMMAND ----------

from pyspark.sql.functions import lit, current_timestamp

# Gerar inventário completo
databases = [row.databaseName for row in spark.sql("SHOW DATABASES IN hive_metastore").collect()]

inventario = []
for db_name in databases:
    if db_name == "default":
        continue
    try:
        tables = spark.sql(f"SHOW TABLES IN hive_metastore.{db_name}").collect()
        for tbl in tables:
            tbl_name = tbl.tableName
            try:
                detail = spark.sql(f"DESCRIBE DETAIL hive_metastore.{db_name}.{tbl_name}").collect()[0]
                inventario.append({
                    "database": db_name,
                    "table": tbl_name,
                    "format": detail.format,
                    "location": detail.location,
                    "num_files": detail.numFiles,
                    "size_bytes": detail.sizeInBytes
                })
            except Exception as e:
                inventario.append({
                    "database": db_name,
                    "table": tbl_name,
                    "format": "ERRO",
                    "location": str(e)[:200],
                    "num_files": 0,
                    "size_bytes": 0
                })
    except Exception as e:
        print(f"Erro ao listar {db_name}: {e}")

if inventario:
    df_inventario = spark.createDataFrame(inventario)
    display(df_inventario)
    print(f"\nTotal: {len(inventario)} tabelas encontradas")
else:
    print("Nenhuma tabela encontrada (execute os módulos anteriores primeiro)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.4 — Classificar tabelas por tipo de migração

# COMMAND ----------

# Tabelas Delta podem ser migradas com CLONE
# Tabelas não-Delta precisam de CTAS

if inventario:
    df_inv = spark.createDataFrame(inventario)

    from pyspark.sql.functions import when, col

    df_classificado = df_inv.withColumn(
        "metodo_migracao",
        when(col("format") == "delta", "DEEP CLONE")
        .when(col("format").isin("parquet", "csv", "json"), "CTAS")
        .otherwise("MANUAL")
    )

    display(df_classificado)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.5 — Inventário de permissões

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Listar permissões em um database específico
# MAGIC -- SHOW GRANTS ON DATABASE hive_metastore.estudos_legado;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercícios
# MAGIC
# MAGIC ### Exercício A
# MAGIC Adicione ao inventário uma coluna com a data da última modificação da tabela.
# MAGIC Dica: DESCRIBE HISTORY retorna essa informação para tabelas Delta.

# COMMAND ----------

# TODO: Seu código aqui

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício B
# MAGIC Salve o inventário como uma tabela Delta para referência futura:
# MAGIC `hive_metastore.estudos_legado.inventario_migracao`

# COMMAND ----------

# TODO: Seu código aqui

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício C — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Qual ferramenta oficial da Databricks auxilia na migração em massa do hive_metastore para Unity Catalog?
# MAGIC
# MAGIC - a) Delta Live Tables
# MAGIC - b) UCX (Unity Catalog Migration Tool)
# MAGIC - c) Auto Loader
# MAGIC - d) Databricks CLI
# MAGIC
# MAGIC > _Sua resposta: ___

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpoint
# MAGIC
# MAGIC - [ ] Inventário de databases gerado
# MAGIC - [ ] Inventário de tabelas com formato e tamanho
# MAGIC - [ ] Tabelas classificadas por método de migração
# MAGIC - [ ] Exercícios A, B e C completados
