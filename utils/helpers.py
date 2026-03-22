# Databricks notebook source

# MAGIC %md
# MAGIC # Helpers — Funções Auxiliares Reutilizáveis
# MAGIC
# MAGIC Execute com `%run ../utils/helpers` no início de qualquer notebook para ter acesso às funções.

# COMMAND ----------

from pyspark.sql.functions import col, count, when, round as spark_round

def resumo_tabela(tabela_nome):
    """Imprime resumo rápido de uma tabela: contagem, colunas, nulos."""
    df = spark.table(tabela_nome)
    total = df.count()
    colunas = len(df.columns)

    print(f"{'='*50}")
    print(f"Tabela: {tabela_nome}")
    print(f"Registros: {total:,}")
    print(f"Colunas: {colunas}")
    print(f"{'='*50}")

    # Contagem de nulos por coluna
    nulos = df.select([
        count(when(col(c).isNull(), c)).alias(c)
        for c in df.columns
    ])

    print("\nNulos por coluna:")
    nulos.show(truncate=False)

    return df

# COMMAND ----------

def comparar_tabelas(tabela_a, tabela_b):
    """Compara duas tabelas: contagem, schema e amostra."""
    df_a = spark.table(tabela_a)
    df_b = spark.table(tabela_b)

    count_a = df_a.count()
    count_b = df_b.count()

    print(f"{'='*50}")
    print(f"Comparação: {tabela_a} vs {tabela_b}")
    print(f"{'='*50}")
    print(f"Contagem A: {count_a:,}")
    print(f"Contagem B: {count_b:,}")
    print(f"Match: {'SIM' if count_a == count_b else 'NÃO'}")

    # Schema
    schema_match = df_a.schema == df_b.schema
    print(f"Schema match: {'SIM' if schema_match else 'NÃO'}")

    if not schema_match:
        cols_a = set(df_a.columns)
        cols_b = set(df_b.columns)
        print(f"  Apenas em A: {cols_a - cols_b}")
        print(f"  Apenas em B: {cols_b - cols_a}")

# COMMAND ----------

def listar_datasets_exemplo():
    """Lista datasets disponíveis no Databricks para prática."""
    datasets = dbutils.fs.ls("/databricks-datasets/")
    print("Datasets disponíveis para prática:\n")
    for d in datasets[:20]:
        print(f"  {d.name}")
    print(f"\n... e mais {len(datasets) - 20} datasets")

# COMMAND ----------

print("Helpers carregados com sucesso!")
print("Funções disponíveis: resumo_tabela(), comparar_tabelas(), listar_datasets_exemplo()")
