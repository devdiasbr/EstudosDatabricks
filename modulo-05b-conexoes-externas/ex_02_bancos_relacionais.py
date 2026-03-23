# Databricks notebook source

# MAGIC %md
# MAGIC # Exercício 2 — Conexão com Bancos Relacionais (JDBC)
# MAGIC
# MAGIC **Módulo 05-B — Conexões com Fontes Externas**
# MAGIC
# MAGIC **Objetivo:** Conectar o Databricks a SQL Server, PostgreSQL, MySQL e Oracle via JDBC.
# MAGIC Entender leitura, escrita, pushdown e boas práticas de performance.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conceitos-chave: JDBC no Databricks
# MAGIC
# MAGIC | Conceito | Descrição |
# MAGIC |---------|-----------|
# MAGIC | **JDBC** | Java Database Connectivity — protocolo padrão para conectar a bancos relacionais |
# MAGIC | **Driver** | Biblioteca Java que traduz chamadas JDBC para o banco específico |
# MAGIC | **Pushdown** | O Spark "empurra" filtros/projeções para o banco (otimização) |
# MAGIC | **Partitioned Read** | Leitura paralela dividindo a query por coluna numérica |
# MAGIC | **fetchSize** | Número de linhas buscadas por vez (tuning de performance) |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2.1 — SQL Server / Azure SQL

# COMMAND ----------

# --- SQL Server / Azure SQL ---

# jdbc_url = "jdbc:sqlserver://meu-server.database.windows.net:1433;database=meu_db"
# connection_properties = {
#     "user": dbutils.secrets.get(scope="jdbc", key="sqlserver-user"),
#     "password": dbutils.secrets.get(scope="jdbc", key="sqlserver-pass"),
#     "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
# }

# Leitura simples
# df = spark.read.jdbc(
#     url=jdbc_url,
#     table="dbo.vendas",
#     properties=connection_properties
# )

# Leitura com pushdown (filtra no banco, não no Spark)
# df = spark.read.jdbc(
#     url=jdbc_url,
#     table="(SELECT * FROM dbo.vendas WHERE ano = 2024) AS subquery",
#     properties=connection_properties
# )

print("SQL Server — leitura via JDBC com pushdown")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2.2 — PostgreSQL

# COMMAND ----------

# --- PostgreSQL ---

# jdbc_url = "jdbc:postgresql://meu-host:5432/meu_db"
# connection_properties = {
#     "user": dbutils.secrets.get(scope="jdbc", key="pg-user"),
#     "password": dbutils.secrets.get(scope="jdbc", key="pg-pass"),
#     "driver": "org.postgresql.Driver"
# }

# Leitura simples
# df = spark.read.jdbc(url=jdbc_url, table="public.clientes", properties=connection_properties)

# Leitura PARTICIONADA (paralela) — ideal para tabelas grandes
# df = spark.read.jdbc(
#     url=jdbc_url,
#     table="public.pedidos",
#     column="id",               # coluna numérica para particionar
#     lowerBound=1,              # valor mínimo
#     upperBound=1000000,        # valor máximo
#     numPartitions=10,          # número de leituras paralelas
#     properties=connection_properties
# )

print("PostgreSQL — leitura simples e particionada")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2.3 — MySQL

# COMMAND ----------

# --- MySQL ---

# jdbc_url = "jdbc:mysql://meu-host:3306/meu_db"
# connection_properties = {
#     "user": dbutils.secrets.get(scope="jdbc", key="mysql-user"),
#     "password": dbutils.secrets.get(scope="jdbc", key="mysql-pass"),
#     "driver": "com.mysql.cj.jdbc.Driver"
# }

# Leitura
# df = spark.read.jdbc(url=jdbc_url, table="produtos", properties=connection_properties)

# Leitura com fetchSize otimizado
# connection_properties["fetchSize"] = "10000"
# df = spark.read.jdbc(url=jdbc_url, table="log_eventos", properties=connection_properties)

print("MySQL — leitura com fetchSize otimizado")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2.4 — Oracle

# COMMAND ----------

# --- Oracle ---

# jdbc_url = "jdbc:oracle:thin:@//meu-host:1521/ORCL"
# connection_properties = {
#     "user": dbutils.secrets.get(scope="jdbc", key="oracle-user"),
#     "password": dbutils.secrets.get(scope="jdbc", key="oracle-pass"),
#     "driver": "oracle.jdbc.OracleDriver"
# }

# Leitura
# df = spark.read.jdbc(url=jdbc_url, table="SCHEMA.TABELA", properties=connection_properties)

print("Oracle — leitura via JDBC")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2.5 — Escrita de volta para o banco (JDBC Write)

# COMMAND ----------

# --- ESCRITA via JDBC ---
# Modos: overwrite, append, ignore, error

# df_resultado.write.jdbc(
#     url=jdbc_url,
#     table="dbo.resultado_analytics",
#     mode="overwrite",            # ou "append"
#     properties=connection_properties
# )

# Com criação de tabela e truncate
# df_resultado.write \
#     .format("jdbc") \
#     .option("url", jdbc_url) \
#     .option("dbtable", "dbo.resultado_analytics") \
#     .option("user", user) \
#     .option("password", password) \
#     .option("truncate", "true") \
#     .mode("overwrite") \
#     .save()

print("JDBC Write — escrita de volta para o banco relacional")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2.6 — Lakehouse Federation (Unity Catalog) ⭐ NOVO

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Lakehouse Federation permite consultar bancos externos
# MAGIC -- como se fossem tabelas do Unity Catalog!
# MAGIC
# MAGIC -- PASSO 1: Criar Connection
# MAGIC -- CREATE CONNECTION sqlserver_conn
# MAGIC -- TYPE SQLSERVER
# MAGIC -- OPTIONS (
# MAGIC --   host = 'meu-server.database.windows.net',
# MAGIC --   port = '1433',
# MAGIC --   user = secret('jdbc', 'sqlserver-user'),
# MAGIC --   password = secret('jdbc', 'sqlserver-pass')
# MAGIC -- );
# MAGIC
# MAGIC -- PASSO 2: Criar Foreign Catalog
# MAGIC -- CREATE FOREIGN CATALOG sqlserver_catalog
# MAGIC -- USING CONNECTION sqlserver_conn
# MAGIC -- OPTIONS (database = 'meu_db');
# MAGIC
# MAGIC -- PASSO 3: Consultar como tabela normal!
# MAGIC -- SELECT * FROM sqlserver_catalog.dbo.vendas LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Resumo: JDBC URLs e Drivers
# MAGIC
# MAGIC | Banco | JDBC URL | Driver Class | Porta Padrão |
# MAGIC |-------|----------|-------------|--------------|
# MAGIC | **SQL Server** | `jdbc:sqlserver://host:1433;database=db` | `com.microsoft.sqlserver.jdbc.SQLServerDriver` | 1433 |
# MAGIC | **PostgreSQL** | `jdbc:postgresql://host:5432/db` | `org.postgresql.Driver` | 5432 |
# MAGIC | **MySQL** | `jdbc:mysql://host:3306/db` | `com.mysql.cj.jdbc.Driver` | 3306 |
# MAGIC | **Oracle** | `jdbc:oracle:thin:@//host:1521/SID` | `oracle.jdbc.OracleDriver` | 1521 |
# MAGIC
# MAGIC ### Dicas de Performance
# MAGIC
# MAGIC | Dica | Descrição |
# MAGIC |------|-----------|
# MAGIC | **Pushdown** | Use subquery no parâmetro `table` para filtrar no banco |
# MAGIC | **Partitioned Read** | Use `column`, `lowerBound`, `upperBound`, `numPartitions` |
# MAGIC | **fetchSize** | Aumente para tabelas grandes (padrão: 10 no Oracle, 0 no MySQL) |
# MAGIC | **Caching** | Salve em Delta depois de ler — evite JDBC repetido |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercícios

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício A — Complete o código JDBC
# MAGIC
# MAGIC ```python
# MAGIC # Conectar ao PostgreSQL e ler a tabela "clientes" com leitura particionada
# MAGIC df = spark.read.jdbc(
# MAGIC     url = "jdbc:____://____:____/meubanco",
# MAGIC     table = "____",
# MAGIC     column = "____",
# MAGIC     lowerBound = ____,
# MAGIC     upperBound = ____,
# MAGIC     numPartitions = ____,
# MAGIC     properties = {"user": "___", "password": "___", "driver": "___"}
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício B — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Um engenheiro precisa ler 50 milhões de registros de uma tabela SQL Server.
# MAGIC Qual abordagem otimiza a performance de leitura?
# MAGIC
# MAGIC - a) `spark.read.jdbc(url, table, properties)` sem parâmetros adicionais
# MAGIC - b) `spark.read.jdbc(url, table, column="id", lowerBound=1, upperBound=50000000, numPartitions=20, properties)`
# MAGIC - c) `spark.read.format("csv").load(jdbc_url)`
# MAGIC - d) `dbutils.fs.cp(jdbc_url, "/mnt/data/")`
# MAGIC
# MAGIC > _Sua resposta: ___
# MAGIC >
# MAGIC > **Gabarito:** b) Leitura particionada divide a carga em 20 leituras paralelas.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício C — Exam-Style
# MAGIC
# MAGIC **Pergunta:** O que é "predicate pushdown" no contexto de JDBC?
# MAGIC
# MAGIC - a) O Spark envia os dados filtrados de volta ao banco
# MAGIC - b) O banco executa os filtros antes de enviar dados ao Spark
# MAGIC - c) O Spark armazena os filtros em cache para reutilização
# MAGIC - d) O JDBC comprime os dados durante a transferência
# MAGIC
# MAGIC > _Sua resposta: ___
# MAGIC >
# MAGIC > **Gabarito:** b) Pushdown faz o banco filtrar/projetar antes de enviar ao Spark.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício D — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Qual recurso do Unity Catalog permite consultar tabelas de bancos externos
# MAGIC como se fossem tabelas locais?
# MAGIC
# MAGIC - a) External Tables
# MAGIC - b) Foreign Catalogs (Lakehouse Federation)
# MAGIC - c) Delta Sharing
# MAGIC - d) Mount Points
# MAGIC
# MAGIC > _Sua resposta: ___
# MAGIC >
# MAGIC > **Gabarito:** b) Lakehouse Federation com Foreign Catalogs.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício E — Prático
# MAGIC
# MAGIC Crie um diagrama (texto) mostrando o fluxo de dados:
# MAGIC
# MAGIC 1. Banco SQL Server (origem) → JDBC Read → DataFrame Spark → Delta Table (bronze)
# MAGIC 2. Inclua: pushdown, fetchSize, partitioned read, e checkpoint
# MAGIC
# MAGIC ```
# MAGIC [SQL Server] --JDBC--> [Spark DataFrame] --write--> [Delta Bronze]
# MAGIC     │                       │
# MAGIC     │ pushdown              │ partitioned read
# MAGIC     │ fetchSize             │ numPartitions=10
# MAGIC ```
