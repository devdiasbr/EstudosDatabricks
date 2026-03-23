# Databricks notebook source

# MAGIC %md
# MAGIC # Exercício 4 — CDC: Change Data Capture com DLT
# MAGIC
# MAGIC **Módulo 5 — Pipelines de Produção**
# MAGIC
# MAGIC **Objetivo:** Entender CDC (Change Data Capture) e o comando `APPLY CHANGES INTO`
# MAGIC do Delta Live Tables. **Tema cobrado no exame!**
# MAGIC
# MAGIC > **NOTA:** Este notebook deve ser executado como pipeline DLT.

# COMMAND ----------

# MAGIC %md
# MAGIC ## O que é CDC?
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────┐                    ┌──────────────────┐
# MAGIC │  BANCO ORIGEM    │   CDC Feed          │   LAKEHOUSE      │
# MAGIC │  (SQL Server,    │ ──────────────────→ │   (Delta Lake)   │
# MAGIC │   PostgreSQL)    │  INSERT/UPDATE/     │                  │
# MAGIC │                  │  DELETE events      │   APPLY CHANGES  │
# MAGIC └──────────────────┘                    └──────────────────┘
# MAGIC ```
# MAGIC
# MAGIC **CDC** captura mudanças incrementais (INSERT, UPDATE, DELETE) de um banco fonte
# MAGIC e aplica no destino, mantendo a tabela sincronizada.
# MAGIC
# MAGIC ### Ferramentas comuns de CDC:
# MAGIC
# MAGIC | Ferramenta | Cloud | Método |
# MAGIC |-----------|-------|--------|
# MAGIC | **Debezium** | Multi-cloud | Log-based (WAL/binlog) |
# MAGIC | **AWS DMS** | AWS | Replicação contínua |
# MAGIC | **Azure Data Factory** | Azure | Watermark / CT |
# MAGIC | **Fivetran / Airbyte** | Multi-cloud | Managed CDC |
# MAGIC | **Kafka Connect** | Multi-cloud | Streaming CDC |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Conceitos-chave
# MAGIC
# MAGIC | Conceito | Descrição |
# MAGIC |---------|-----------|
# MAGIC | **CDC Feed** | Stream de eventos (INSERT, UPDATE, DELETE) do banco fonte |
# MAGIC | **SCD Type 1** | Sobrescreve o registro antigo (sem histórico) |
# MAGIC | **SCD Type 2** | Mantém histórico com colunas `__START_AT` e `__END_AT` |
# MAGIC | **APPLY CHANGES INTO** | Comando DLT que aplica CDC automaticamente |
# MAGIC | **Sequence Key** | Coluna que ordena os eventos (timestamp, sequence_id) |
# MAGIC | **Primary Key** | Identifica unicamente o registro (para merge) |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Formato típico de um CDC Feed
# MAGIC
# MAGIC ```json
# MAGIC {"operation": "INSERT", "id": 1, "nome": "Maria", "cidade": "SP", "timestamp": "2024-01-01T10:00:00"}
# MAGIC {"operation": "UPDATE", "id": 1, "nome": "Maria", "cidade": "RJ", "timestamp": "2024-01-02T14:30:00"}
# MAGIC {"operation": "INSERT", "id": 2, "nome": "João",  "cidade": "BH", "timestamp": "2024-01-02T15:00:00"}
# MAGIC {"operation": "DELETE", "id": 1, "nome": "Maria", "cidade": "RJ", "timestamp": "2024-01-03T09:00:00"}
# MAGIC ```
# MAGIC
# MAGIC Colunas essenciais:
# MAGIC - **operation**: tipo da mudança (INSERT, UPDATE, DELETE)
# MAGIC - **Primary key**: identifica o registro (ex: `id`)
# MAGIC - **Sequence key**: ordena cronologicamente (ex: `timestamp`)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## APPLY CHANGES INTO — SCD Type 1 (sem histórico)

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, expr

# COMMAND ----------

# --- BRONZE: Ingerir CDC feed ---

@dlt.table(
    comment="CDC feed bruto de clientes"
)
def bronze_clientes_cdc():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/data/cdc/clientes/")
    )

# COMMAND ----------

# --- SILVER: Aplicar CDC com SCD Type 1 ---

dlt.create_streaming_table(
    name="silver_clientes",
    comment="Clientes atualizados via CDC (SCD Type 1 — sem histórico)"
)

dlt.apply_changes(
    target="silver_clientes",
    source="bronze_clientes_cdc",
    keys=["id"],                          # Primary key
    sequence_by=col("timestamp"),          # Ordenação temporal
    apply_as_deletes=expr("operation = 'DELETE'"),
    except_column_list=["operation", "timestamp"],  # Remover colunas de controle
    stored_as_scd_type=1                   # Tipo 1: sobrescreve
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### O que acontece internamente (SCD Type 1):
# MAGIC
# MAGIC ```
# MAGIC Evento 1 (INSERT id=1, Maria, SP):
# MAGIC   Tabela: | id | nome  | cidade |
# MAGIC           | 1  | Maria | SP     |
# MAGIC
# MAGIC Evento 2 (UPDATE id=1, Maria, RJ):
# MAGIC   Tabela: | id | nome  | cidade |
# MAGIC           | 1  | Maria | RJ     |   ← SP sobrescrito por RJ
# MAGIC
# MAGIC Evento 3 (DELETE id=1):
# MAGIC   Tabela: (vazia)                    ← registro removido
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## APPLY CHANGES INTO — SCD Type 2 (com histórico)

# COMMAND ----------

# --- SILVER: Aplicar CDC com SCD Type 2 (mantém histórico) ---

dlt.create_streaming_table(
    name="silver_clientes_historico",
    comment="Clientes com histórico completo via CDC (SCD Type 2)"
)

dlt.apply_changes(
    target="silver_clientes_historico",
    source="bronze_clientes_cdc",
    keys=["id"],
    sequence_by=col("timestamp"),
    apply_as_deletes=expr("operation = 'DELETE'"),
    except_column_list=["operation", "timestamp"],
    stored_as_scd_type=2                   # Tipo 2: mantém histórico
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### O que acontece internamente (SCD Type 2):
# MAGIC
# MAGIC ```
# MAGIC Evento 1 (INSERT id=1, Maria, SP, ts=10:00):
# MAGIC   | id | nome  | cidade | __START_AT | __END_AT |
# MAGIC   | 1  | Maria | SP     | 10:00      | NULL     |
# MAGIC
# MAGIC Evento 2 (UPDATE id=1, Maria, RJ, ts=14:30):
# MAGIC   | id | nome  | cidade | __START_AT | __END_AT |
# MAGIC   | 1  | Maria | SP     | 10:00      | 14:30    |  ← fechado
# MAGIC   | 1  | Maria | RJ     | 14:30      | NULL     |  ← atual
# MAGIC
# MAGIC Colunas automáticas:
# MAGIC   __START_AT  → quando o registro ficou ativo
# MAGIC   __END_AT    → quando foi substituído (NULL = registro atual)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Simulando CDC com Faker (para praticar)

# COMMAND ----------

# from faker import Faker
# import json
# from datetime import datetime, timedelta
#
# fake = Faker("pt_BR")
# Faker.seed(42)
#
# cdc_events = []
# base_time = datetime(2024, 1, 1, 8, 0, 0)
#
# # Gerar 100 clientes (INSERT)
# for i in range(1, 101):
#     cdc_events.append({
#         "operation": "INSERT",
#         "id": i,
#         "nome": fake.name(),
#         "email": fake.email(),
#         "cidade": fake.city(),
#         "estado": fake.state_abbr(),
#         "plano": fake.random_element(["basic", "premium", "enterprise"]),
#         "timestamp": (base_time + timedelta(minutes=i)).isoformat()
#     })
#
# # Gerar 30 UPDATEs (mudança de cidade/plano)
# for i in range(30):
#     client_id = fake.random_int(min=1, max=100)
#     cdc_events.append({
#         "operation": "UPDATE",
#         "id": client_id,
#         "nome": fake.name(),
#         "email": fake.email(),
#         "cidade": fake.city(),
#         "estado": fake.state_abbr(),
#         "plano": fake.random_element(["basic", "premium", "enterprise"]),
#         "timestamp": (base_time + timedelta(hours=24, minutes=i)).isoformat()
#     })
#
# # Gerar 10 DELETEs
# for i in range(10):
#     client_id = fake.random_int(min=80, max=100)
#     cdc_events.append({
#         "operation": "DELETE",
#         "id": client_id,
#         "nome": "",
#         "email": "",
#         "cidade": "",
#         "estado": "",
#         "plano": "",
#         "timestamp": (base_time + timedelta(hours=48, minutes=i)).isoformat()
#     })
#
# # Salvar como JSON na landing zone
# df_cdc = spark.createDataFrame(cdc_events)
# df_cdc.write.mode("overwrite").json("/data/cdc/clientes/")
# print(f"CDC feed gerado: {len(cdc_events)} eventos (100 INSERT, 30 UPDATE, 10 DELETE)")

print("Gerador de CDC feed com Faker (descomente para executar)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## SCD Type 1 vs SCD Type 2 — Comparativo
# MAGIC
# MAGIC | Aspecto | SCD Type 1 | SCD Type 2 |
# MAGIC |---------|-----------|-----------|
# MAGIC | Histórico | ❌ Não mantém | ✅ Mantém completo |
# MAGIC | Registro atualizado | Sobrescrito | Nova linha com `__START_AT` / `__END_AT` |
# MAGIC | Tamanho da tabela | Menor (1 linha por chave) | Maior (N linhas por chave) |
# MAGIC | Query "estado atual" | `SELECT *` | `WHERE __END_AT IS NULL` |
# MAGIC | Uso | Dimensões que não precisam de auditoria | Dimensões com requisito de auditoria |
# MAGIC | DLT Param | `stored_as_scd_type=1` | `stored_as_scd_type=2` |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Parâmetros do APPLY CHANGES INTO
# MAGIC
# MAGIC | Parâmetro | Obrigatório | Descrição |
# MAGIC |-----------|:-----------:|-----------|
# MAGIC | `target` | ✅ | Tabela destino (criada com `create_streaming_table`) |
# MAGIC | `source` | ✅ | Tabela fonte (CDC feed) |
# MAGIC | `keys` | ✅ | Lista de colunas da primary key |
# MAGIC | `sequence_by` | ✅ | Coluna para ordenar eventos |
# MAGIC | `apply_as_deletes` | ❌ | Expressão que identifica DELETEs |
# MAGIC | `apply_as_truncates` | ❌ | Expressão que identifica TRUNCATEs |
# MAGIC | `except_column_list` | ❌ | Colunas a excluir do destino |
# MAGIC | `stored_as_scd_type` | ❌ | `1` (padrão) ou `2` |
# MAGIC | `column_list` | ❌ | Colunas a incluir (oposto de except) |
# MAGIC | `track_history_column_list` | ❌ | Colunas que geram nova versão no SCD2 |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## SQL Syntax (alternativa ao Python)
# MAGIC
# MAGIC ```sql
# MAGIC -- Criar streaming table
# MAGIC CREATE OR REFRESH STREAMING TABLE silver_clientes;
# MAGIC
# MAGIC -- Aplicar CDC (SCD Type 1)
# MAGIC APPLY CHANGES INTO LIVE.silver_clientes
# MAGIC FROM STREAM(LIVE.bronze_clientes_cdc)
# MAGIC KEYS (id)
# MAGIC SEQUENCE BY timestamp
# MAGIC APPLY AS DELETE WHEN operation = 'DELETE'
# MAGIC COLUMNS * EXCEPT (operation, timestamp)
# MAGIC STORED AS SCD TYPE 1;
# MAGIC
# MAGIC -- SCD Type 2
# MAGIC APPLY CHANGES INTO LIVE.silver_clientes_historico
# MAGIC FROM STREAM(LIVE.bronze_clientes_cdc)
# MAGIC KEYS (id)
# MAGIC SEQUENCE BY timestamp
# MAGIC APPLY AS DELETE WHEN operation = 'DELETE'
# MAGIC COLUMNS * EXCEPT (operation, timestamp)
# MAGIC STORED AS SCD TYPE 2;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercícios

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício A — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Qual comando DLT é usado para processar um CDC feed e aplicar
# MAGIC INSERT, UPDATE e DELETE em uma tabela Delta?
# MAGIC
# MAGIC - a) `MERGE INTO`
# MAGIC - b) `COPY INTO`
# MAGIC - c) `APPLY CHANGES INTO`
# MAGIC - d) `CREATE STREAMING TABLE`
# MAGIC
# MAGIC > _Sua resposta: ___
# MAGIC >
# MAGIC > **Gabarito:** c) `APPLY CHANGES INTO` é o comando DLT específico para CDC.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício B — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Em SCD Type 2 com `APPLY CHANGES INTO`, como identificar
# MAGIC o registro ATUAL de um cliente?
# MAGIC
# MAGIC - a) `WHERE version = (SELECT MAX(version))`
# MAGIC - b) `WHERE __END_AT IS NULL`
# MAGIC - c) `WHERE is_current = TRUE`
# MAGIC - d) `WHERE rownum = 1`
# MAGIC
# MAGIC > _Sua resposta: ___
# MAGIC >
# MAGIC > **Gabarito:** b) `__END_AT IS NULL` indica o registro ativo no SCD Type 2 do DLT.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício C — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Qual parâmetro de `apply_changes` define a ordenação temporal dos eventos CDC?
# MAGIC
# MAGIC - a) `order_by`
# MAGIC - b) `timestamp_col`
# MAGIC - c) `sequence_by`
# MAGIC - d) `sort_key`
# MAGIC
# MAGIC > _Sua resposta: ___
# MAGIC >
# MAGIC > **Gabarito:** c) `sequence_by` define a coluna usada para ordenar os eventos.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício D — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Qual é a diferença entre SCD Type 1 e SCD Type 2?
# MAGIC
# MAGIC - a) SCD1 mantém histórico, SCD2 não
# MAGIC - b) SCD1 sobrescreve registros, SCD2 mantém versões históricas
# MAGIC - c) SCD1 é para streaming, SCD2 é para batch
# MAGIC - d) SCD1 usa MERGE, SCD2 usa INSERT
# MAGIC
# MAGIC > _Sua resposta: ___
# MAGIC >
# MAGIC > **Gabarito:** b) SCD1 sobrescreve (sem histórico), SCD2 cria novas linhas com __START_AT/__END_AT.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício E — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Antes de usar `apply_changes()`, o que é necessário criar primeiro?
# MAGIC
# MAGIC - a) Uma Delta table com `CREATE TABLE`
# MAGIC - b) Uma streaming table com `dlt.create_streaming_table()`
# MAGIC - c) Uma view com `dlt.view()`
# MAGIC - d) Um DataFrame com `spark.readStream`
# MAGIC
# MAGIC > _Sua resposta: ___
# MAGIC >
# MAGIC > **Gabarito:** b) A tabela destino deve ser criada com `dlt.create_streaming_table()`.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício F — Prático: Complete o código
# MAGIC
# MAGIC ```python
# MAGIC import dlt
# MAGIC from pyspark.sql.functions import col, expr
# MAGIC
# MAGIC # Bronze: ingerir CDC de pedidos
# MAGIC @dlt.table()
# MAGIC def bronze_pedidos_cdc():
# MAGIC     return (
# MAGIC         spark.readStream.format("____")
# MAGIC         .option("cloudFiles.format", "____")
# MAGIC         .load("/data/cdc/pedidos/")
# MAGIC     )
# MAGIC
# MAGIC # Silver: aplicar CDC com SCD Type 2
# MAGIC dlt.____("silver_pedidos")
# MAGIC
# MAGIC dlt.apply_changes(
# MAGIC     target="____",
# MAGIC     source="____",
# MAGIC     keys=["____"],
# MAGIC     sequence_by=col("____"),
# MAGIC     apply_as_deletes=expr("____ = '____'"),
# MAGIC     stored_as_scd_type=____
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpoint
# MAGIC
# MAGIC - [ ] CDC conceito entendido (INSERT/UPDATE/DELETE feed)
# MAGIC - [ ] APPLY CHANGES INTO — Python syntax
# MAGIC - [ ] APPLY CHANGES INTO — SQL syntax
# MAGIC - [ ] SCD Type 1 vs Type 2 diferenciados
# MAGIC - [ ] `__START_AT` / `__END_AT` no SCD Type 2
# MAGIC - [ ] `sequence_by`, `keys`, `apply_as_deletes` entendidos
# MAGIC - [ ] Exercícios A-F completados
