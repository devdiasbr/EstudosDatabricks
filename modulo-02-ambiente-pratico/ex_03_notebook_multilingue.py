# Databricks notebook source

# MAGIC %md
# MAGIC # Exercício 3 — Notebook Multilíngue e Widgets
# MAGIC
# MAGIC **Módulo 2 — Ambiente Prático**
# MAGIC
# MAGIC **Vídeo de apoio:** [Cadernos do Databricks](https://www.youtube.com/playlist?list=PLnxUgOpvXnWzTkha0174lXfES019PQz9U) — Vídeo 6
# MAGIC
# MAGIC **Objetivo:** Dominar magic commands, múltiplas linguagens e widgets.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 — Magic Commands
# MAGIC
# MAGIC Este notebook demonstra os principais magic commands:
# MAGIC - `%python` — célula Python (padrão)
# MAGIC - `%sql` — célula SQL
# MAGIC - `%scala` — célula Scala
# MAGIC - `%md` — célula Markdown
# MAGIC - `%fs` — dbutils.fs (file system)
# MAGIC - `%run` — executar outro notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ### Célula Python (padrão)

# COMMAND ----------

# Python — criar DataFrame
dados = [
    (1, "Ana", "Engenharia", 8500.00),
    (2, "Bruno", "Dados", 9200.00),
    (3, "Carla", "Engenharia", 8800.00),
    (4, "Diego", "Dados", 9500.00),
    (5, "Elena", "Produto", 8100.00)
]
colunas = ["id", "nome", "departamento", "salario"]

df = spark.createDataFrame(dados, colunas)
df.createOrReplaceTempView("funcionarios")

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Célula SQL — consultando a Temp View criada no Python

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SQL — consultar os dados criados no Python
# MAGIC SELECT departamento,
# MAGIC        COUNT(*) as total,
# MAGIC        ROUND(AVG(salario), 2) as salario_medio
# MAGIC FROM funcionarios
# MAGIC GROUP BY departamento
# MAGIC ORDER BY salario_medio DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Célula File System

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /databricks-datasets/

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 — Widgets (Parâmetros)
# MAGIC
# MAGIC Widgets permitem parametrizar notebooks para reuso:

# COMMAND ----------

# Criar widgets
dbutils.widgets.text("departamento", "Dados", "Filtrar por departamento")
dbutils.widgets.dropdown("ordenar_por", "salario", ["salario", "nome", "id"], "Ordenar por")

# COMMAND ----------

# Usar os valores dos widgets
dept = dbutils.widgets.get("departamento")
ordem = dbutils.widgets.get("ordenar_por")

print(f"Filtrando por: {dept}")
print(f"Ordenando por: {ordem}")

df_filtrado = df.filter(df.departamento == dept).orderBy(ordem)
display(df_filtrado)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Widgets também funcionam em SQL com getArgument
# MAGIC SELECT * FROM funcionarios
# MAGIC WHERE departamento = getArgument('departamento')

# COMMAND ----------

# Limpar widgets (execute quando terminar)
# dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3 — Exercícios para praticar
# MAGIC
# MAGIC ### Exercício A
# MAGIC Crie um DataFrame de `produtos` com colunas (id, nome, categoria, preco) e registre como Temp View.
# MAGIC Depois, consulte com `%sql` para encontrar o produto mais caro por categoria.

# COMMAND ----------

# TODO: Seu código aqui
# dados_produtos = [...]
# ...

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Sua query aqui
# MAGIC -- SELECT categoria, ...

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício B
# MAGIC Crie um widget dropdown com as categorias do seu DataFrame de produtos.
# MAGIC Filtre os resultados usando o widget.

# COMMAND ----------

# TODO: Seu código aqui
# dbutils.widgets.dropdown(...)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício C — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Qual magic command permite executar um notebook dentro de outro e compartilhar variáveis?
# MAGIC
# MAGIC - a) `%import`
# MAGIC - b) `%run`
# MAGIC - c) `%exec`
# MAGIC - d) `%include`
# MAGIC
# MAGIC > _Sua resposta: ___

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpoint
# MAGIC
# MAGIC - [ ] Notebook com Python, SQL, %fs e %md funcionando
# MAGIC - [ ] Temp View criada em Python e consultada em SQL
# MAGIC - [ ] Widgets criados e utilizados
# MAGIC - [ ] Exercícios A, B e C completados
