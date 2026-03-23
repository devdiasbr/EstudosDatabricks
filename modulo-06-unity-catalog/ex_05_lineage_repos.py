# Databricks notebook source

# MAGIC %md
# MAGIC # Exercício 5 — Data Lineage e Databricks Repos (Git Integration)
# MAGIC
# MAGIC **Módulo 6 — Unity Catalog & Governança**
# MAGIC
# MAGIC **Objetivo:** Entender Data Lineage no Unity Catalog e a integração Git com Repos.
# MAGIC **Ambos caem no exame!**

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Parte 1: Data Lineage

# COMMAND ----------

# MAGIC %md
# MAGIC ## O que é Data Lineage?
# MAGIC
# MAGIC ```
# MAGIC ┌─────────┐     ┌─────────┐     ┌─────────┐     ┌──────────┐
# MAGIC │ bronze. │ ──→ │ silver. │ ──→ │ gold.   │ ──→ │Dashboard │
# MAGIC │ vendas  │     │ vendas  │     │ receita │     │ (DBSQL)  │
# MAGIC └─────────┘     └─────────┘     └─────────┘     └──────────┘
# MAGIC       │
# MAGIC       └──→ ┌─────────┐
# MAGIC            │ silver. │ ──→ ...
# MAGIC            │ estoque │
# MAGIC            └─────────┘
# MAGIC ```
# MAGIC
# MAGIC **Data Lineage** mostra **de onde vêm os dados e para onde vão**.
# MAGIC - **Upstream**: tabelas/fontes que alimentam uma tabela
# MAGIC - **Downstream**: tabelas/dashboards que dependem de uma tabela
# MAGIC - **Column-level lineage**: rastreamento por coluna individual

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lineage no Unity Catalog
# MAGIC
# MAGIC ### Como funciona?
# MAGIC - O Unity Catalog **captura automaticamente** o lineage de queries executadas
# MAGIC - Visível na **UI**: Catalog Explorer → selecione tabela → aba **Lineage**
# MAGIC - Mostra: tabelas upstream, tabelas downstream, notebooks, workflows
# MAGIC
# MAGIC ### O que é rastreado?
# MAGIC
# MAGIC | Tipo | Rastreado? | Exemplo |
# MAGIC |------|:---------:|---------|
# MAGIC | Table → Table | ✅ | `INSERT INTO silver SELECT FROM bronze` |
# MAGIC | Column → Column | ✅ | `silver.nome` vem de `bronze.nome` |
# MAGIC | Notebook → Table | ✅ | Qual notebook criou/escreveu na tabela |
# MAGIC | Workflow → Table | ✅ | Qual job escreveu na tabela |
# MAGIC | Dashboard → Table | ✅ | Qual dashboard lê a tabela |
# MAGIC | DLT Pipeline → Table | ✅ | Rastreamento automático |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Usando Lineage via SQL/API

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Ver detalhes de uma tabela (inclui info de lineage)
# MAGIC -- DESCRIBE EXTENDED catalog.schema.tabela;

# MAGIC -- System tables para lineage (se habilitadas)
# MAGIC -- SELECT * FROM system.access.table_lineage
# MAGIC -- WHERE target_table_name = 'vendas_gold'
# MAGIC -- ORDER BY event_time DESC;

# MAGIC -- Column lineage
# MAGIC -- SELECT * FROM system.access.column_lineage
# MAGIC -- WHERE target_table_name = 'vendas_gold';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Casos de uso do Lineage
# MAGIC
# MAGIC | Caso | Como o Lineage ajuda |
# MAGIC |------|---------------------|
# MAGIC | **Impact Analysis** | "Se eu mudar esta tabela, quem é afetado?" → ver downstream |
# MAGIC | **Root Cause** | "De onde veio este dado errado?" → ver upstream |
# MAGIC | **Compliance** | "Quais tabelas usam dados de PII?" → rastrear coluna CPF |
# MAGIC | **Documentation** | Auto-documentação do fluxo de dados |
# MAGIC | **Migration** | "Quais pipelines preciso atualizar?" → ver dependências |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Parte 2: Databricks Repos (Git Integration)

# COMMAND ----------

# MAGIC %md
# MAGIC ## O que são Repos?
# MAGIC
# MAGIC **Repos** é a integração nativa do Databricks com Git, permitindo:
# MAGIC - **Versionar notebooks** com Git (GitHub, GitLab, Azure DevOps, Bitbucket)
# MAGIC - **Branching** — trabalhar em branches separadas
# MAGIC - **Pull Requests** — code review antes de merge
# MAGIC - **CI/CD** — automatizar deploy de notebooks
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────┐              ┌──────────────────┐
# MAGIC │   Databricks     │  git clone   │   GitHub/GitLab  │
# MAGIC │   Workspace      │ ◄──────────→ │   Azure DevOps   │
# MAGIC │                  │  push/pull   │   Bitbucket      │
# MAGIC │   /Repos/user/   │              │                  │
# MAGIC │     meu-projeto/ │              │   meu-projeto/   │
# MAGIC │       notebook1  │              │     notebook1.py │
# MAGIC │       notebook2  │              │     notebook2.py │
# MAGIC └──────────────────┘              └──────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configurando Repos
# MAGIC
# MAGIC ### Passo 1: Conectar Git Provider
# MAGIC 1. **User Settings → Git Integration**
# MAGIC 2. Selecionar provider (GitHub, GitLab, etc.)
# MAGIC 3. Informar **Personal Access Token (PAT)** ou usar OAuth
# MAGIC
# MAGIC ### Passo 2: Clonar repositório
# MAGIC 1. **Workspace → Repos → Add Repo**
# MAGIC 2. Colar URL do repositório Git
# MAGIC 3. O Databricks clona o repo no Workspace
# MAGIC
# MAGIC ### Passo 3: Workflow de desenvolvimento
# MAGIC ```
# MAGIC 1. Criar branch:       feature/nova-pipeline
# MAGIC 2. Editar notebooks:   no Databricks (ou IDE local)
# MAGIC 3. Commit + Push:      via UI do Repos
# MAGIC 4. Pull Request:       no GitHub/GitLab
# MAGIC 5. Code Review:        time revisa
# MAGIC 6. Merge → main:       deploy automático (CI/CD)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Repos: Funcionalidades
# MAGIC
# MAGIC | Feature | Descrição |
# MAGIC |---------|-----------|
# MAGIC | **Clone** | Clonar repo Git no Workspace |
# MAGIC | **Branch** | Criar/trocar branches |
# MAGIC | **Commit** | Salvar mudanças locais |
# MAGIC | **Push/Pull** | Sincronizar com remote |
# MAGIC | **Diff** | Ver diferenças entre versões |
# MAGIC | **Conflict Resolution** | Resolver conflitos de merge |
# MAGIC | **Arbitrary Files** | Suporta `.py`, `.sql`, `.md`, `.yaml`, etc. |
# MAGIC | **Folders** | Estrutura de pastas do Git preservada |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Repos vs Workspace (sem Git)
# MAGIC
# MAGIC | Aspecto | Workspace (sem Git) | Repos (com Git) |
# MAGIC |---------|-------------------|-----------------|
# MAGIC | Versionamento | ❌ Apenas revisão interna | ✅ Git completo |
# MAGIC | Branching | ❌ | ✅ |
# MAGIC | Code Review | ❌ | ✅ Pull Requests |
# MAGIC | CI/CD | ❌ | ✅ GitHub Actions, Azure Pipelines |
# MAGIC | Colaboração | Compartilhar notebook | Git workflow (branch + PR) |
# MAGIC | Backup | ❌ No Databricks apenas | ✅ Remote Git (GitHub, etc.) |
# MAGIC | Recomendação | Prototipagem rápida | ⭐ Produção |

# COMMAND ----------

# MAGIC %md
# MAGIC ## CI/CD com Repos — Fluxo de Produção
# MAGIC
# MAGIC ```
# MAGIC ┌─────────┐    ┌─────────┐    ┌──────────┐    ┌───────────┐
# MAGIC │   Dev   │ →  │  PR +   │ →  │  CI/CD   │ →  │   Prod    │
# MAGIC │ Branch  │    │ Review  │    │ Pipeline │    │ Workspace │
# MAGIC │         │    │         │    │ (Actions)│    │           │
# MAGIC │ feature/│    │ main    │    │ test +   │    │ /Repos/   │
# MAGIC │ my-feat │    │         │    │ deploy   │    │ prod/     │
# MAGIC └─────────┘    └─────────┘    └──────────┘    └───────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Ferramentas de CI/CD comuns:
# MAGIC
# MAGIC | Ferramenta | Cloud | Integração |
# MAGIC |-----------|-------|------------|
# MAGIC | **GitHub Actions** | Multi-cloud | Databricks CLI + API |
# MAGIC | **Azure DevOps** | Azure | Azure Pipelines + Databricks |
# MAGIC | **GitLab CI** | Multi-cloud | Databricks CLI |
# MAGIC | **Databricks Asset Bundles (DABs)** | Multi-cloud | Deploy nativo ⭐ |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercícios

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício A — Exam-Style (Lineage)
# MAGIC
# MAGIC **Pergunta:** Um engenheiro precisa descobrir quais dashboards serão afetados
# MAGIC se ele alterar a coluna `valor` da tabela `silver.vendas`. Qual feature usar?
# MAGIC
# MAGIC - a) DESCRIBE HISTORY
# MAGIC - b) Data Lineage no Unity Catalog (downstream analysis)
# MAGIC - c) SHOW GRANTS
# MAGIC - d) Query History
# MAGIC
# MAGIC > _Sua resposta: ___
# MAGIC >
# MAGIC > **Gabarito:** b) Lineage mostra todas as dependências downstream (tabelas, dashboards).

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício B — Exam-Style (Lineage)
# MAGIC
# MAGIC **Pergunta:** O Data Lineage do Unity Catalog rastreia automaticamente qual informação?
# MAGIC
# MAGIC - a) Apenas relações entre tabelas
# MAGIC - b) Relações entre tabelas, colunas, notebooks e workflows
# MAGIC - c) Apenas queries executadas por admins
# MAGIC - d) Apenas pipelines DLT
# MAGIC
# MAGIC > _Sua resposta: ___
# MAGIC >
# MAGIC > **Gabarito:** b) O lineage rastreia tabelas, colunas, notebooks, workflows e dashboards.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício C — Exam-Style (Repos)
# MAGIC
# MAGIC **Pergunta:** Qual é a principal vantagem de usar Databricks Repos para
# MAGIC gerenciar notebooks em produção?
# MAGIC
# MAGIC - a) Notebooks executam mais rápido
# MAGIC - b) Permite versionamento com Git, branching e code review via PRs
# MAGIC - c) Habilita Unity Catalog automaticamente
# MAGIC - d) Reduz o custo dos clusters
# MAGIC
# MAGIC > _Sua resposta: ___
# MAGIC >
# MAGIC > **Gabarito:** b) Repos habilita Git workflow completo (branch, PR, CI/CD).

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício D — Exam-Style (Repos)
# MAGIC
# MAGIC **Pergunta:** Um time quer implementar CI/CD para seus pipelines Databricks.
# MAGIC Qual combinação de ferramentas é mais adequada?
# MAGIC
# MAGIC - a) Databricks Repos + Databricks Asset Bundles + GitHub Actions
# MAGIC - b) dbutils.fs.cp + cron jobs
# MAGIC - c) Export manual de notebooks + upload via UI
# MAGIC - d) Delta Sharing + SQL Warehouses
# MAGIC
# MAGIC > _Sua resposta: ___
# MAGIC >
# MAGIC > **Gabarito:** a) Repos (Git) + DABs (deploy) + Actions (CI/CD) é o padrão moderno.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício E — Prático: Workflow Git
# MAGIC
# MAGIC Ordene os passos de um workflow de desenvolvimento com Repos:
# MAGIC
# MAGIC - [ ] Merge to main após aprovação
# MAGIC - [ ] Criar branch `feature/nova-tabela`
# MAGIC - [ ] Push para remote
# MAGIC - [ ] Abrir Pull Request para code review
# MAGIC - [ ] Editar notebook no Databricks
# MAGIC - [ ] Commit das mudanças
# MAGIC - [ ] CI/CD pipeline faz deploy em produção
# MAGIC
# MAGIC > _Numere de 1 a 7_
# MAGIC >
# MAGIC > **Gabarito:** 1-Branch, 2-Edit, 3-Commit, 4-Push, 5-PR, 6-Merge, 7-CI/CD

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpoint
# MAGIC
# MAGIC - [ ] Data Lineage: upstream, downstream, column-level
# MAGIC - [ ] Lineage no Catalog Explorer (UI)
# MAGIC - [ ] System tables de lineage
# MAGIC - [ ] Repos: clone, branch, commit, push/pull
# MAGIC - [ ] Repos vs Workspace (sem Git)
# MAGIC - [ ] CI/CD: GitHub Actions, Azure Pipelines, DABs
# MAGIC - [ ] Exercícios A-E completados
