# Databricks notebook source

# MAGIC %md
# MAGIC # Exercício 4 — Delta Sharing
# MAGIC
# MAGIC **Módulo 6 — Unity Catalog & Governança**
# MAGIC
# MAGIC **Objetivo:** Entender Delta Sharing — compartilhamento seguro de dados
# MAGIC entre organizações, plataformas e clouds. **Tema cobrado no exame!**

# COMMAND ----------

# MAGIC %md
# MAGIC ## O que é Delta Sharing?
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────┐                         ┌──────────────────┐
# MAGIC │   PROVIDER        │   Delta Sharing          │   RECIPIENT       │
# MAGIC │   (Quem tem       │ ────────────────────→    │   (Quem recebe    │
# MAGIC │    os dados)      │   Protocolo aberto       │    os dados)      │
# MAGIC │                   │   Read-only               │                   │
# MAGIC │   Databricks      │                          │   Databricks      │
# MAGIC │   Unity Catalog   │                          │   Spark           │
# MAGIC │                   │                          │   Pandas          │
# MAGIC │                   │                          │   Power BI        │
# MAGIC │                   │                          │   Tableau         │
# MAGIC └──────────────────┘                         └──────────────────┘
# MAGIC ```
# MAGIC
# MAGIC **Delta Sharing** é um protocolo **aberto** para compartilhar dados de forma segura:
# MAGIC - **Sem copiar dados** — o recipient lê diretamente da fonte
# MAGIC - **Cross-platform** — não precisa ser Databricks dos dois lados
# MAGIC - **Cross-cloud** — Azure ↔ AWS ↔ GCP
# MAGIC - **Governado** — controle granular via Unity Catalog

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Conceitos-chave
# MAGIC
# MAGIC | Conceito | Descrição |
# MAGIC |---------|-----------|
# MAGIC | **Share** | Coleção de tabelas/views compartilhadas |
# MAGIC | **Provider** | Quem cria e gerencia o share (tem os dados) |
# MAGIC | **Recipient** | Quem recebe acesso ao share (lê os dados) |
# MAGIC | **Activation Link** | URL que o recipient usa para baixar credenciais |
# MAGIC | **Open Sharing** | Compartilhamento com qualquer plataforma (token-based) |
# MAGIC | **Databricks-to-Databricks** | Compartilhamento nativo entre workspaces Databricks |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Criando um Share (Provider side)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- PASSO 1: Criar um Share
# MAGIC -- CREATE SHARE vendas_share
# MAGIC -- COMMENT 'Dados de vendas para parceiros';

# MAGIC -- PASSO 2: Adicionar tabelas ao Share
# MAGIC -- ALTER SHARE vendas_share
# MAGIC -- ADD TABLE catalog_prod.analytics.vendas_gold;

# MAGIC -- ALTER SHARE vendas_share
# MAGIC -- ADD TABLE catalog_prod.analytics.produtos
# MAGIC -- WITH HISTORY;  -- permite Time Travel no recipient

# MAGIC -- Adicionar com alias (renomear para o recipient)
# MAGIC -- ALTER SHARE vendas_share
# MAGIC -- ADD TABLE catalog_prod.analytics.clientes
# MAGIC -- AS parceiro.dados.clientes;

# MAGIC -- Ver o conteúdo do Share
# MAGIC -- SHOW ALL IN SHARE vendas_share;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Criando um Recipient

# COMMAND ----------

# MAGIC %sql
# MAGIC -- PASSO 3: Criar Recipient (Open Sharing — qualquer plataforma)
# MAGIC -- CREATE RECIPIENT parceiro_xpto
# MAGIC -- COMMENT 'Parceiro XPTO - equipe de analytics';

# MAGIC -- Databricks-to-Databricks (sharing ID do workspace recipient)
# MAGIC -- CREATE RECIPIENT parceiro_databricks
# MAGIC -- USING ID 'aws:us-east-1:workspace-id-12345';

# MAGIC -- PASSO 4: Conceder acesso ao Share
# MAGIC -- GRANT SELECT ON SHARE vendas_share TO RECIPIENT parceiro_xpto;

# MAGIC -- Ver recipients
# MAGIC -- SHOW GRANTS ON SHARE vendas_share;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Consumindo um Share (Recipient side)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Opção 1: Databricks-to-Databricks

# COMMAND ----------

# MAGIC %sql
# MAGIC -- No workspace RECIPIENT, criar catalog a partir do share
# MAGIC -- CREATE CATALOG parceiro_dados
# MAGIC -- USING SHARE provider_workspace.vendas_share;
# MAGIC
# MAGIC -- Consultar normalmente!
# MAGIC -- SELECT * FROM parceiro_dados.dados.clientes LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Opção 2: Open Sharing (Python/Pandas — qualquer plataforma)

# COMMAND ----------

# --- Recipient usando Python (fora do Databricks) ---
# pip install delta-sharing

# import delta_sharing
#
# # Arquivo de profile baixado do activation link
# profile_file = "config.share"
#
# # Listar shares disponíveis
# client = delta_sharing.SharingClient(profile_file)
# print(client.list_all_tables())
#
# # Ler como Pandas DataFrame
# table_url = f"{profile_file}#vendas_share.analytics.vendas_gold"
# df = delta_sharing.load_as_pandas(table_url)
# print(df.head())
#
# # Ler como Spark DataFrame
# df_spark = delta_sharing.load_as_spark(table_url)
# df_spark.show()

print("Delta Sharing — consumo via Python (fora do Databricks)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Opção 3: Power BI / Tableau
# MAGIC
# MAGIC 1. Baixar o **activation link** (gera arquivo `config.share`)
# MAGIC 2. No Power BI: usar **Delta Sharing connector**
# MAGIC 3. No Tableau: usar **Databricks connector** ou **Delta Sharing connector**
# MAGIC 4. Apontar para o share e selecionar tabelas
# MAGIC
# MAGIC > **Nota:** O recipient **nunca** tem acesso ao storage — apenas lê via protocolo Delta Sharing.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Delta Sharing vs Outras Formas de Compartilhar Dados
# MAGIC
# MAGIC | Método | Cópia de dados? | Cross-org? | Cross-cloud? | Governança |
# MAGIC |--------|:--------------:|:----------:|:------------:|:----------:|
# MAGIC | **Delta Sharing** | ❌ Zero-copy | ✅ | ✅ | ✅ UC |
# MAGIC | Export CSV/Parquet | ✅ Cópia | ✅ | ✅ | ❌ Manual |
# MAGIC | Cross-catalog (UC) | ❌ | ❌ Mesmo org | ❌ Mesmo cloud | ✅ UC |
# MAGIC | ETL replicação | ✅ Cópia | ✅ | ✅ | ❌ Manual |
# MAGIC | APIs customizadas | ✅ Cópia | ✅ | ✅ | ⚠️ Manual |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Gerenciamento de Shares

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Listar shares
# MAGIC -- SHOW SHARES;

# MAGIC -- Detalhes de um share
# MAGIC -- DESCRIBE SHARE vendas_share;

# MAGIC -- Remover tabela de um share
# MAGIC -- ALTER SHARE vendas_share
# MAGIC -- REMOVE TABLE catalog_prod.analytics.vendas_gold;

# MAGIC -- Revogar acesso
# MAGIC -- REVOKE SELECT ON SHARE vendas_share FROM RECIPIENT parceiro_xpto;

# MAGIC -- Deletar
# MAGIC -- DROP RECIPIENT parceiro_xpto;
# MAGIC -- DROP SHARE vendas_share;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercícios

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício A — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Qual é a principal vantagem do Delta Sharing sobre exportar dados como CSV?
# MAGIC
# MAGIC - a) É mais rápido para processar
# MAGIC - b) Compartilha dados sem criar cópias (zero-copy)
# MAGIC - c) Suporta mais formatos de arquivo
# MAGIC - d) Funciona apenas dentro do Databricks
# MAGIC
# MAGIC > _Sua resposta: ___
# MAGIC >
# MAGIC > **Gabarito:** b) Delta Sharing é zero-copy — o recipient lê direto da fonte.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício B — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Uma empresa precisa compartilhar dados com um parceiro que usa
# MAGIC Spark on-premise (não tem Databricks). Qual método de Delta Sharing usar?
# MAGIC
# MAGIC - a) Databricks-to-Databricks sharing
# MAGIC - b) Open Sharing com activation link
# MAGIC - c) Unity Catalog cross-catalog
# MAGIC - d) External Locations
# MAGIC
# MAGIC > _Sua resposta: ___
# MAGIC >
# MAGIC > **Gabarito:** b) Open Sharing funciona com qualquer plataforma via token/profile file.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício C — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Qual comando SQL cria um share e adiciona uma tabela a ele?
# MAGIC
# MAGIC - a) `CREATE SHARE s; ALTER SHARE s ADD TABLE t;`
# MAGIC - b) `CREATE SHARE s; GRANT TABLE t TO SHARE s;`
# MAGIC - c) `CREATE SHARE s; INSERT INTO SHARE s TABLE t;`
# MAGIC - d) `SHARE TABLE t AS s;`
# MAGIC
# MAGIC > _Sua resposta: ___
# MAGIC >
# MAGIC > **Gabarito:** a) `CREATE SHARE` + `ALTER SHARE ADD TABLE` é a sintaxe correta.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício D — Exam-Style
# MAGIC
# MAGIC **Pergunta:** O recipient de um Delta Share tem acesso direto ao storage account
# MAGIC onde os dados estão armazenados?
# MAGIC
# MAGIC - a) Sim, recebe as credenciais do storage
# MAGIC - b) Sim, via External Location
# MAGIC - c) Não, lê apenas via protocolo Delta Sharing (sem acesso ao storage)
# MAGIC - d) Depende da configuração do provider
# MAGIC
# MAGIC > _Sua resposta: ___
# MAGIC >
# MAGIC > **Gabarito:** c) O recipient NUNCA tem acesso ao storage — lê via protocolo apenas.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício E — Prático
# MAGIC
# MAGIC Ordene os passos para compartilhar dados via Delta Sharing:
# MAGIC
# MAGIC - [ ] Criar um RECIPIENT
# MAGIC - [ ] Criar um SHARE
# MAGIC - [ ] GRANT SELECT ON SHARE ao RECIPIENT
# MAGIC - [ ] Adicionar tabelas ao SHARE (ALTER SHARE ADD TABLE)
# MAGIC - [ ] Recipient baixa activation link e consome dados
# MAGIC
# MAGIC > _Numere de 1 a 5_
# MAGIC >
# MAGIC > **Gabarito:** 2, 1, 4 (SHARE → ADD TABLE), 3 (GRANT), 5 (consume)
# MAGIC > Na verdade: 1-SHARE, 2-ADD TABLE, 3-RECIPIENT, 4-GRANT, 5-consume

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpoint
# MAGIC
# MAGIC - [ ] Delta Sharing conceito (zero-copy, cross-platform)
# MAGIC - [ ] Share, Provider, Recipient entendidos
# MAGIC - [ ] Criar Share + adicionar tabelas (SQL)
# MAGIC - [ ] Criar Recipient (Open vs Databricks-to-Databricks)
# MAGIC - [ ] Consumir share (Python, SQL, BI tools)
# MAGIC - [ ] Delta Sharing vs outros métodos de compartilhamento
# MAGIC - [ ] Exercícios A-E completados
