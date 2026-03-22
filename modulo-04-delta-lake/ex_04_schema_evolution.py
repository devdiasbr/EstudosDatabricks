# Databricks notebook source

# MAGIC %md
# MAGIC # Exercício 4 — Schema Enforcement vs Schema Evolution
# MAGIC
# MAGIC **Módulo 4 — Delta Lake**
# MAGIC
# MAGIC **Objetivo:** Entender como Delta Lake trata mudanças de schema.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 — Schema Enforcement (padrão)
# MAGIC
# MAGIC Delta Lake rejeita escritas que não correspondem ao schema da tabela.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tabela existente com schema definido
# MAGIC DESCRIBE certificacao_bronze.produtos;

# COMMAND ----------

# Tentar inserir dados com coluna extra (VAI DAR ERRO)
try:
    df_extra = spark.createDataFrame(
        [(8, "Tablet", "eletronicos", 2500.00, 40, True, "2024-03-01")],
        ["id", "nome", "categoria", "preco", "estoque", "ativo", "data_lancamento"]
    )
    df_extra.write.format("delta").mode("append").saveAsTable("certificacao_bronze.produtos")
except Exception as e:
    print(f"ERRO (esperado): {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 — Schema Evolution (mergeSchema)
# MAGIC
# MAGIC Permite adicionar novas colunas automaticamente durante a escrita.

# COMMAND ----------

# Com mergeSchema = true, a nova coluna é aceita
df_extra = spark.createDataFrame(
    [(8, "Tablet Samsung", "eletronicos", 2500.00, 40, True, "2024-03-01")],
    ["id", "nome", "categoria", "preco", "estoque", "ativo", "data_lancamento"]
)

df_extra.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("certificacao_bronze.produtos")

print("Escrita com mergeSchema realizada com sucesso!")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verificar que a nova coluna foi adicionada
# MAGIC DESCRIBE certificacao_bronze.produtos;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Registros antigos terão NULL na nova coluna
# MAGIC SELECT id, nome, data_lancamento FROM certificacao_bronze.produtos;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.3 — Schema Evolution via SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Adicionar coluna manualmente
# MAGIC ALTER TABLE certificacao_bronze.produtos ADD COLUMNS (peso_kg DOUBLE);

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE certificacao_bronze.produtos;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.4 — overwriteSchema (substituir schema completo)

# COMMAND ----------

# overwriteSchema substitui o schema inteiro (use com cuidado!)
df_novo_schema = spark.createDataFrame(
    [(1, "Notebook", 3500.00, "eletronicos")],
    ["id", "produto", "valor", "tipo"]
)

# Isso substitui TODO o schema da tabela
# df_novo_schema.write.format("delta") \
#     .mode("overwrite") \
#     .option("overwriteSchema", "true") \
#     .saveAsTable("certificacao_bronze.produtos")

print("overwriteSchema comentado por segurança — descomente para testar.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercícios
# MAGIC
# MAGIC ### Exercício A
# MAGIC Crie uma tabela `certificacao_bronze.teste_schema` com 3 colunas.
# MAGIC Tente fazer append com 4 colunas (sem mergeSchema) — observe o erro.
# MAGIC Depois faça com mergeSchema — observe o sucesso.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Seu código aqui

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício B — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Qual opção permite adicionar novas colunas automaticamente ao fazer append em uma tabela Delta?
# MAGIC
# MAGIC - a) `.option("autoSchema", "true")`
# MAGIC - b) `.option("mergeSchema", "true")`
# MAGIC - c) `.option("evolveSchema", "true")`
# MAGIC - d) `.option("schemaHints", "auto")`
# MAGIC
# MAGIC > _Sua resposta: ___

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício C — Exam-Style
# MAGIC
# MAGIC **Pergunta:** O que acontece com os registros existentes quando uma nova coluna é adicionada via mergeSchema?
# MAGIC
# MAGIC - a) São deletados
# MAGIC - b) Recebem valor padrão definido pelo usuário
# MAGIC - c) Recebem NULL na nova coluna
# MAGIC - d) O append falha
# MAGIC
# MAGIC > _Sua resposta: ___

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpoint
# MAGIC
# MAGIC - [ ] Schema Enforcement: erro ao tentar escrever com schema diferente
# MAGIC - [ ] Schema Evolution: mergeSchema adicionou nova coluna
# MAGIC - [ ] ALTER TABLE ADD COLUMNS executado
# MAGIC - [ ] Entendido overwriteSchema vs mergeSchema
# MAGIC - [ ] Exercícios A, B e C completados
