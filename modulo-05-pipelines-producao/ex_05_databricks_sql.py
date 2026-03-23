# Databricks notebook source

# MAGIC %md
# MAGIC # Exercício 5 — Databricks SQL (SQL Warehouses, Dashboards, Alerts)
# MAGIC
# MAGIC **Módulo 5 — Pipelines de Produção**
# MAGIC
# MAGIC **Objetivo:** Entender SQL Warehouses, queries, dashboards e alerts no Databricks SQL.
# MAGIC **Tema cobrado no exame!**

# COMMAND ----------

# MAGIC %md
# MAGIC ## O que é Databricks SQL (DBSQL)?
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────────────────────────────────────────────┐
# MAGIC │                    DATABRICKS SQL                        │
# MAGIC │                                                          │
# MAGIC │  ┌────────────┐  ┌────────────┐  ┌────────────────────┐ │
# MAGIC │  │   SQL       │  │ Dashboards │  │   Alerts           │ │
# MAGIC │  │ Warehouses  │  │ (Visões)   │  │ (Notificações)     │ │
# MAGIC │  └──────┬─────┘  └──────┬─────┘  └────────┬───────────┘ │
# MAGIC │         │               │                  │             │
# MAGIC │         └───────────────┼──────────────────┘             │
# MAGIC │                         │                                │
# MAGIC │                    ┌────┴─────┐                          │
# MAGIC │                    │  Delta   │                          │
# MAGIC │                    │  Lake    │                          │
# MAGIC │                    └──────────┘                          │
# MAGIC └──────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC **DBSQL** é a interface SQL nativa do Databricks para analistas e engenheiros
# MAGIC executarem queries, criarem dashboards e configurarem alerts — tudo sobre Delta Lake.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5.1 — SQL Warehouses
# MAGIC
# MAGIC ### O que são?
# MAGIC Clusters otimizados **exclusivamente para SQL**. Diferente de All-Purpose/Job Clusters.
# MAGIC
# MAGIC ### Tipos de SQL Warehouse
# MAGIC
# MAGIC | Tipo | Descrição | Custo | Startup |
# MAGIC |------|-----------|-------|---------|
# MAGIC | **Classic** | Compute dedicado | Médio | ~5 min |
# MAGIC | **Pro** | + Query profiling, melhores otimizações | Alto | ~5 min |
# MAGIC | **Serverless** | Compute gerenciado pela Databricks | Variável | **Segundos** ⚡ |
# MAGIC
# MAGIC ### Configurações importantes
# MAGIC
# MAGIC | Configuração | Descrição |
# MAGIC |-------------|-----------|
# MAGIC | **Size** | T-shirt sizing (2X-Small → 4X-Large) — determina recursos |
# MAGIC | **Auto Stop** | Desliga após X minutos sem uso (economia!) |
# MAGIC | **Min/Max Clusters** | Scaling horizontal (múltiplos clusters para concorrência) |
# MAGIC | **Spot Instances** | Usar instâncias spot para economia (até 90% mais barato) |
# MAGIC | **Channel** | Current (estável) vs Preview (últimas features) |

# COMMAND ----------

# MAGIC %md
# MAGIC ### SQL Warehouse vs Clusters Interativos
# MAGIC
# MAGIC | Aspecto | All-Purpose Cluster | SQL Warehouse |
# MAGIC |---------|-------------------|---------------|
# MAGIC | Linguagens | Python, SQL, Scala, R | **Somente SQL** |
# MAGIC | Uso | Notebooks, dev, exploração | Queries, dashboards, BI |
# MAGIC | Otimização | Genérica | **Otimizada para SQL** (Photon) |
# MAGIC | Concorrência | Limitada | Alta (multi-cluster scaling) |
# MAGIC | Serverless | ❌ (só DBR 13.3+) | ✅ Startup em segundos |
# MAGIC | Custo típico | Mais caro (running 24/7 dev) | Mais barato (auto-stop) |
# MAGIC | Unity Catalog | ✅ | ✅ |
# MAGIC | Exame | "Quando usar All-Purpose?" | "Quando usar SQL Warehouse?" |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5.2 — SQL Editor (Queries)
# MAGIC
# MAGIC O **SQL Editor** é onde você escreve e executa queries no DBSQL.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Exemplo de queries no SQL Editor:

# MAGIC -- 1. Explorar tabelas
# MAGIC -- SHOW TABLES IN certificacao_bronze;

# MAGIC -- 2. Query com agregação
# MAGIC -- SELECT
# MAGIC --     cidade,
# MAGIC --     COUNT(*) as total_clientes,
# MAGIC --     AVG(idade) as idade_media
# MAGIC -- FROM certificacao_bronze.clientes
# MAGIC -- GROUP BY cidade
# MAGIC -- ORDER BY total_clientes DESC
# MAGIC -- LIMIT 10;

# MAGIC -- 3. Query parametrizada (usa {{ parametro }} no SQL Editor)
# MAGIC -- SELECT * FROM certificacao_bronze.vendas
# MAGIC -- WHERE data_venda >= '{{ data_inicio }}'
# MAGIC --   AND data_venda <= '{{ data_fim }}'
# MAGIC --   AND canal = '{{ canal }}';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Funcionalidades do SQL Editor
# MAGIC
# MAGIC | Feature | Descrição |
# MAGIC |---------|-----------|
# MAGIC | **Autocomplete** | Sugere tabelas, colunas, funções |
# MAGIC | **Query History** | Histórico de todas as queries executadas |
# MAGIC | **Snippets** | Templates reutilizáveis de SQL |
# MAGIC | **Parameters** | `{{ parametro }}` — queries dinâmicas |
# MAGIC | **Scheduled Queries** | Executar queries em horários programados |
# MAGIC | **Query Profile** | Plano de execução visual (Pro/Serverless) |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5.3 — Dashboards

# COMMAND ----------

# MAGIC %md
# MAGIC ### Criando um Dashboard (passo a passo)
# MAGIC
# MAGIC 1. **Escreva queries** no SQL Editor (cada query = um widget)
# MAGIC 2. Vá em **SQL → Dashboards → Create Dashboard**
# MAGIC 3. **Adicione widgets** (cada widget = uma query + visualização)
# MAGIC 4. **Tipos de visualização**: tabela, bar chart, line chart, pie chart, counter, map, etc.
# MAGIC 5. **Parâmetros compartilhados**: um filtro afeta múltiplos widgets
# MAGIC 6. **Schedule**: atualizar automaticamente (a cada 1h, diário, etc.)
# MAGIC 7. **Compartilhar**: com usuários/grupos via permissões do Workspace
# MAGIC
# MAGIC ### Dashboard vs Notebook
# MAGIC
# MAGIC | Aspecto | Notebook | Dashboard |
# MAGIC |---------|----------|-----------|
# MAGIC | Público | Engenheiros / Cientistas | Analistas / Stakeholders |
# MAGIC | Linguagem | Multi-language | SQL only |
# MAGIC | Interatividade | Código | Filtros visuais |
# MAGIC | Refresh | Manual | Scheduled |
# MAGIC | Uso | Dev / Exploração | Reporting / Apresentação |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5.4 — Alerts (Alertas)

# COMMAND ----------

# MAGIC %md
# MAGIC ### O que são Alerts?
# MAGIC
# MAGIC Monitoram o resultado de uma query e **disparam notificações** quando uma condição é atendida.
# MAGIC
# MAGIC ### Configuração:
# MAGIC
# MAGIC 1. Crie uma **query que retorna um valor** (ex: `SELECT COUNT(*) FROM erros`)
# MAGIC 2. Vá em **SQL → Alerts → Create Alert**
# MAGIC 3. Configure:
# MAGIC    - **Query**: selecione a query
# MAGIC    - **Trigger condition**: `Value column` `>` `threshold`
# MAGIC    - **Refresh**: a cada 5 min, 1h, etc.
# MAGIC    - **Notification**: Email, Slack, Webhook
# MAGIC
# MAGIC ### Exemplos práticos:
# MAGIC
# MAGIC | Alert | Query | Condição |
# MAGIC |-------|-------|----------|
# MAGIC | Pipeline falhou | `SELECT COUNT(*) FROM audit WHERE status='FAILED' AND date=TODAY` | `> 0` |
# MAGIC | Volume anormal | `SELECT COUNT(*) FROM bronze.vendas WHERE data = TODAY` | `< 1000` |
# MAGIC | Dados atrasados | `SELECT DATEDIFF(NOW(), MAX(ingest_time)) FROM bronze.eventos` | `> 60` (min) |
# MAGIC | Qualidade ruim | `SELECT COUNT(*) FROM silver.vendas WHERE valor IS NULL` | `> 100` |
# MAGIC
# MAGIC ### Alert States:
# MAGIC
# MAGIC | Estado | Significado | Cor |
# MAGIC |--------|------------|-----|
# MAGIC | **OK** | Condição NÃO atendida | 🟢 Verde |
# MAGIC | **TRIGGERED** | Condição atendida | 🔴 Vermelho |
# MAGIC | **UNKNOWN** | Query não executou ainda | ⚪ Cinza |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5.5 — Query History e Auditing
# MAGIC
# MAGIC | Feature | Descrição |
# MAGIC |---------|-----------|
# MAGIC | **Query History** | Todas as queries executadas (quem, quando, duração, status) |
# MAGIC | **Query Profile** | Plano de execução visual (gargalos, shuffles) |
# MAGIC | **Audit Logs** | Logs de acesso e modificação (Unity Catalog) |
# MAGIC | **System Tables** | `system.access.audit` — queries programáticas sobre acessos |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercícios

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício A — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Qual tipo de compute é otimizado exclusivamente para queries SQL
# MAGIC e suporta alta concorrência com multi-cluster scaling?
# MAGIC
# MAGIC - a) All-Purpose Cluster
# MAGIC - b) Job Cluster
# MAGIC - c) SQL Warehouse
# MAGIC - d) Single Node Cluster
# MAGIC
# MAGIC > _Sua resposta: ___
# MAGIC >
# MAGIC > **Gabarito:** c) SQL Warehouses são otimizados para SQL com suporte a scaling horizontal.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício B — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Qual tipo de SQL Warehouse tem startup em segundos?
# MAGIC
# MAGIC - a) Classic
# MAGIC - b) Pro
# MAGIC - c) Serverless
# MAGIC - d) Standard
# MAGIC
# MAGIC > _Sua resposta: ___
# MAGIC >
# MAGIC > **Gabarito:** c) Serverless — compute gerenciado pela Databricks, startup instantâneo.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício C — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Um analista precisa criar um relatório visual que se atualiza automaticamente
# MAGIC a cada hora. Qual recurso do Databricks SQL ele deve usar?
# MAGIC
# MAGIC - a) Notebook com widget
# MAGIC - b) Delta Live Tables pipeline
# MAGIC - c) Dashboard com scheduled refresh
# MAGIC - d) Job com email notification
# MAGIC
# MAGIC > _Sua resposta: ___
# MAGIC >
# MAGIC > **Gabarito:** c) Dashboards suportam scheduled refresh para atualização automática.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício D — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Um engenheiro quer ser notificado quando o pipeline de ingestão
# MAGIC parar de processar dados por mais de 1 hora. Qual recurso usar?
# MAGIC
# MAGIC - a) DLT Expectations
# MAGIC - b) SQL Alert com query monitorando o timestamp de ingestão
# MAGIC - c) Cluster auto-scaling
# MAGIC - d) Unity Catalog audit logs
# MAGIC
# MAGIC > _Sua resposta: ___
# MAGIC >
# MAGIC > **Gabarito:** b) Alerts monitoram queries e disparam notificações baseadas em condições.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício E — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Qual configuração de SQL Warehouse ajuda a reduzir custos
# MAGIC quando o warehouse não está em uso?
# MAGIC
# MAGIC - a) Aumentar o tamanho (size)
# MAGIC - b) Habilitar Auto Stop
# MAGIC - c) Usar o canal Preview
# MAGIC - d) Aumentar Max Clusters
# MAGIC
# MAGIC > _Sua resposta: ___
# MAGIC >
# MAGIC > **Gabarito:** b) Auto Stop desliga o warehouse após X minutos de inatividade.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício F — Prático: Planeje um Dashboard
# MAGIC
# MAGIC Crie o design de um dashboard de vendas com 4 widgets:
# MAGIC
# MAGIC | Widget | Tipo Visualização | Query SQL (esboço) |
# MAGIC |--------|------------------|-------------------|
# MAGIC | 1. Total de vendas hoje | Counter | `SELECT SUM(valor) FROM ...` |
# MAGIC | 2. Vendas por categoria | ___ | `SELECT ...` |
# MAGIC | 3. Top 5 vendedores | ___ | `SELECT ...` |
# MAGIC | 4. Trend diário (últimos 30 dias) | ___ | `SELECT ...` |
# MAGIC
# MAGIC > _Complete as queries e tipos de visualização_

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpoint
# MAGIC
# MAGIC - [ ] SQL Warehouses: Classic vs Pro vs Serverless
# MAGIC - [ ] SQL Warehouse vs All-Purpose Cluster
# MAGIC - [ ] SQL Editor: queries, parâmetros, snippets
# MAGIC - [ ] Dashboards: criação, widgets, schedule, compartilhamento
# MAGIC - [ ] Alerts: configuração, estados, notificações
# MAGIC - [ ] Query History e Profile
# MAGIC - [ ] Exercícios A-F completados
