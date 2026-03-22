# Databricks notebook source

# MAGIC %md
# MAGIC # Exercício 5 — Validação Pós-Migração
# MAGIC
# MAGIC **Módulo 8 — Migração Hive → Unity Catalog**
# MAGIC
# MAGIC **Objetivo:** Garantir integridade dos dados após a migração.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.1 — Função de validação

# COMMAND ----------

def validar_migracao(tabela_origem, tabela_destino, verbose=True):
    """
    Valida a migração comparando origem e destino.
    Retorna True se tudo OK, False se houver divergência.
    """
    erros = []

    # 1. Comparar contagem de registros
    count_origem = spark.sql(f"SELECT COUNT(*) as c FROM {tabela_origem}").collect()[0].c
    count_destino = spark.sql(f"SELECT COUNT(*) as c FROM {tabela_destino}").collect()[0].c

    if count_origem != count_destino:
        erros.append(f"CONTAGEM: origem={count_origem}, destino={count_destino}")
    elif verbose:
        print(f"  ✓ Contagem OK: {count_destino} registros")

    # 2. Comparar schema
    schema_origem = spark.table(tabela_origem).schema
    schema_destino = spark.table(tabela_destino).schema

    cols_origem = set([(f.name, str(f.dataType)) for f in schema_origem.fields])
    cols_destino = set([(f.name, str(f.dataType)) for f in schema_destino.fields])

    if cols_origem != cols_destino:
        diff = cols_origem.symmetric_difference(cols_destino)
        erros.append(f"SCHEMA: diferenças em {diff}")
    elif verbose:
        print(f"  ✓ Schema OK: {len(schema_destino.fields)} colunas")

    # 3. Verificar que destino é Delta
    detail = spark.sql(f"DESCRIBE DETAIL {tabela_destino}").collect()[0]
    if detail.format != "delta":
        erros.append(f"FORMATO: destino é {detail.format}, esperado delta")
    elif verbose:
        print(f"  ✓ Formato OK: delta")

    # Resultado
    if erros:
        print(f"  ✗ FALHOU: {tabela_destino}")
        for e in erros:
            print(f"    - {e}")
        return False
    else:
        if verbose:
            print(f"  ✓ MIGRAÇÃO VÁLIDA: {tabela_destino}")
        return True

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.2 — Executar validação

# COMMAND ----------

# Validar tabelas migradas
pares_migracao = [
    ("hive_metastore.estudos_legado.vendas", "empresa_migrada.bronze.vendas"),
    # Adicione mais pares conforme migrou
]

print("=" * 60)
print("RELATÓRIO DE VALIDAÇÃO PÓS-MIGRAÇÃO")
print("=" * 60)

total = 0
sucesso = 0
for origem, destino in pares_migracao:
    total += 1
    print(f"\n[{total}] {origem} → {destino}")
    try:
        if validar_migracao(origem, destino):
            sucesso += 1
    except Exception as e:
        print(f"  ✗ ERRO: {e}")

print(f"\n{'=' * 60}")
print(f"RESULTADO: {sucesso}/{total} tabelas migradas com sucesso")
print(f"{'=' * 60}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.3 — Validação por amostragem

# COMMAND ----------

def validar_amostra(tabela_origem, tabela_destino, coluna_chave="id", amostra=10):
    """Compara registros individuais entre origem e destino."""
    df_origem = spark.table(tabela_origem).orderBy(coluna_chave).limit(amostra)
    df_destino = spark.table(tabela_destino).orderBy(coluna_chave).limit(amostra)

    # Subtract: retorna linhas que estão em um mas não no outro
    diff_1 = df_origem.subtract(df_destino)
    diff_2 = df_destino.subtract(df_origem)

    if diff_1.count() == 0 and diff_2.count() == 0:
        print(f"  ✓ Amostra de {amostra} registros idêntica")
        return True
    else:
        print(f"  ✗ Diferenças encontradas na amostra!")
        print("  Apenas na origem:")
        display(diff_1)
        print("  Apenas no destino:")
        display(diff_2)
        return False

# Executar
for origem, destino in pares_migracao:
    print(f"\nAmostragem: {origem} → {destino}")
    validar_amostra(origem, destino)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercícios
# MAGIC
# MAGIC ### Exercício A
# MAGIC Adicione à função `validar_migracao` uma checagem de checksum (hash das colunas).
# MAGIC Dica: use `md5(concat_ws(',', *))` para gerar hash por linha.

# COMMAND ----------

# TODO: Seu código aqui

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício B
# MAGIC Crie um notebook que gera um relatório completo de migração em formato de tabela,
# MAGIC salvando o resultado em `empresa_migrada.gold.relatorio_migracao`.

# COMMAND ----------

# TODO: Seu código aqui

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício C — Documentar Runbook de Migração
# MAGIC
# MAGIC Preencha o runbook:
# MAGIC
# MAGIC | Etapa | Responsável | Critério de Go/No-Go | Status |
# MAGIC |-------|------------|---------------------|--------|
# MAGIC | 1. Inventário | ___ | ___ | [ ] |
# MAGIC | 2. Migração Bronze | ___ | ___ | [ ] |
# MAGIC | 3. Migração Silver | ___ | ___ | [ ] |
# MAGIC | 4. Migração Gold | ___ | ___ | [ ] |
# MAGIC | 5. Permissões | ___ | ___ | [ ] |
# MAGIC | 6. Validação | ___ | ___ | [ ] |
# MAGIC | 7. Atualizar jobs | ___ | ___ | [ ] |
# MAGIC | 8. Deprecar hive | ___ | ___ | [ ] |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpoint
# MAGIC
# MAGIC - [ ] Função de validação criada e testada
# MAGIC - [ ] Validação por contagem, schema e formato executada
# MAGIC - [ ] Validação por amostragem executada
# MAGIC - [ ] Exercícios A, B e C completados
