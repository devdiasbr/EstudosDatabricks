# Databricks notebook source

# MAGIC %md
# MAGIC # Exercício 1 — Conexão com Cloud Storage
# MAGIC
# MAGIC **Módulo 05-B — Conexões com Fontes Externas**
# MAGIC
# MAGIC **Objetivo:** Conectar o Databricks a Azure ADLS Gen2, AWS S3 e GCP GCS.
# MAGIC Entender as diferenças entre mount, acesso direto e external locations (UC).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visão Geral: Como dados chegam ao Lakehouse?
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────┐
# MAGIC │                    SISTEMAS FONTES                         │
# MAGIC │  ERP │ CRM │ APIs │ IoT │ Logs │ Bancos │ Arquivos        │
# MAGIC └──────────────┬──────────────────────────────────────────────┘
# MAGIC                │
# MAGIC                ▼
# MAGIC ┌─────────────────────────────────────────────────────────────┐
# MAGIC │              LANDING ZONE (Cloud Storage)                   │
# MAGIC │  Azure ADLS Gen2 │ AWS S3 │ GCP GCS                        │
# MAGIC │  Arquivos: CSV, JSON, Parquet, Avro, XML                   │
# MAGIC └──────────────┬──────────────────────────────────────────────┘
# MAGIC                │  Auto Loader / COPY INTO / JDBC
# MAGIC                ▼
# MAGIC ┌─────────────────────────────────────────────────────────────┐
# MAGIC │              DATABRICKS LAKEHOUSE                           │
# MAGIC │  Bronze → Silver → Gold (Medallion Architecture)           │
# MAGIC └─────────────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1.1 — Azure ADLS Gen2 (Azure Data Lake Storage)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Método 1: Acesso Direto com Service Principal (recomendado com UC)
# MAGIC
# MAGIC > No Unity Catalog, o acesso é feito via **External Locations** e **Storage Credentials**.
# MAGIC > Não precisa montar nada!

# COMMAND ----------

# --- ACESSO DIRETO (SEM MOUNT) — Azure ADLS Gen2 ---

# Configuração com Service Principal (OAuth2)
# spark.conf.set(
#     "fs.azure.account.auth.type.<storage-account>.dfs.core.windows.net",
#     "OAuth"
# )
# spark.conf.set(
#     "fs.azure.account.oauth.provider.type.<storage-account>.dfs.core.windows.net",
#     "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
# )
# spark.conf.set(
#     "fs.azure.account.oauth2.client.id.<storage-account>.dfs.core.windows.net",
#     dbutils.secrets.get(scope="azure-kv", key="client-id")
# )
# spark.conf.set(
#     "fs.azure.account.oauth2.client.secret.<storage-account>.dfs.core.windows.net",
#     dbutils.secrets.get(scope="azure-kv", key="client-secret")
# )
# spark.conf.set(
#     "fs.azure.account.oauth2.client.endpoint.<storage-account>.dfs.core.windows.net",
#     "https://login.microsoftonline.com/<tenant-id>/oauth2/token"
# )

# Leitura direta
# df = spark.read.format("csv") \
#     .option("header", "true") \
#     .load("abfss://<container>@<storage-account>.dfs.core.windows.net/landing/vendas/")

print("Azure ADLS Gen2 — acesso direto com Service Principal")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Método 2: Mount Point (legado — NÃO recomendado com Unity Catalog)

# COMMAND ----------

# --- MOUNT POINT (LEGADO) — Azure ADLS Gen2 ---
# Funciona mas NÃO é compatível com Unity Catalog

# dbutils.fs.mount(
#     source = "abfss://<container>@<storage-account>.dfs.core.windows.net/",
#     mount_point = "/mnt/landing",
#     extra_configs = {
#         "fs.azure.account.key.<storage-account>.dfs.core.windows.net":
#             dbutils.secrets.get(scope="azure-kv", key="storage-key")
#     }
# )

# Leitura via mount
# df = spark.read.csv("/mnt/landing/vendas/", header=True)

print("Mount point: método legado, evitar com Unity Catalog")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Método 3: External Location (Unity Catalog) ⭐ RECOMENDADO

# COMMAND ----------

# MAGIC %sql
# MAGIC -- PASSO 1: Criar Storage Credential (admin)
# MAGIC -- CREATE STORAGE CREDENTIAL azure_cred
# MAGIC -- WITH (
# MAGIC --   AZURE_MANAGED_IDENTITY = 'access-connector-resource-id'
# MAGIC -- );
# MAGIC
# MAGIC -- PASSO 2: Criar External Location
# MAGIC -- CREATE EXTERNAL LOCATION landing_azure
# MAGIC -- URL 'abfss://landing@meustorage.dfs.core.windows.net/'
# MAGIC -- WITH (STORAGE CREDENTIAL azure_cred);
# MAGIC
# MAGIC -- PASSO 3: Ler dados
# MAGIC -- SELECT * FROM csv.`abfss://landing@meustorage.dfs.core.windows.net/vendas/`
# MAGIC -- LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1.2 — AWS S3

# COMMAND ----------

# --- AWS S3 — Acesso Direto com Instance Profile ou IAM Role ---

# Método 1: Instance Profile (configurado no cluster)
# Não precisa de código! O cluster já tem acesso via IAM Role.
# df = spark.read.csv("s3://meu-bucket/landing/vendas/", header=True)

# Método 2: Access Key (NÃO recomendado em produção)
# spark.conf.set("fs.s3a.access.key", dbutils.secrets.get(scope="aws", key="access-key"))
# spark.conf.set("fs.s3a.secret.key", dbutils.secrets.get(scope="aws", key="secret-key"))
# df = spark.read.csv("s3a://meu-bucket/landing/vendas/", header=True)

# Método 3: External Location (Unity Catalog) ⭐
# CREATE STORAGE CREDENTIAL aws_cred WITH (AWS_IAM_ROLE = 'arn:aws:iam::role/databricks');
# CREATE EXTERNAL LOCATION landing_s3 URL 's3://meu-bucket/landing/' WITH (STORAGE CREDENTIAL aws_cred);

print("AWS S3 — 3 métodos de acesso")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1.3 — GCP Google Cloud Storage

# COMMAND ----------

# --- GCP GCS — Google Cloud Storage ---

# Método 1: Service Account (configurado no cluster)
# df = spark.read.csv("gs://meu-bucket/landing/vendas/", header=True)

# Método 2: External Location (Unity Catalog) ⭐
# CREATE STORAGE CREDENTIAL gcp_cred WITH (GCP_SERVICE_ACCOUNT = 'sa@project.iam.gserviceaccount.com');
# CREATE EXTERNAL LOCATION landing_gcs URL 'gs://meu-bucket/landing/' WITH (STORAGE CREDENTIAL gcp_cred);

print("GCP GCS — acesso via Service Account ou External Location")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Resumo Comparativo
# MAGIC
# MAGIC | Cloud | Protocolo | URI Exemplo | Autenticação Recomendada |
# MAGIC |-------|-----------|-------------|--------------------------|
# MAGIC | **Azure ADLS** | `abfss://` | `abfss://container@account.dfs.core.windows.net/path` | Managed Identity + External Location |
# MAGIC | **AWS S3** | `s3://` | `s3://bucket-name/path` | IAM Role + External Location |
# MAGIC | **GCP GCS** | `gs://` | `gs://bucket-name/path` | Service Account + External Location |
# MAGIC
# MAGIC ### Mount vs Acesso Direto vs External Location
# MAGIC
# MAGIC | Aspecto | Mount (legado) | Acesso Direto | External Location (UC) |
# MAGIC |---------|---------------|---------------|------------------------|
# MAGIC | Unity Catalog | ❌ Incompatível | ⚠️ Parcial | ✅ Nativo |
# MAGIC | Segurança | ❌ Chave no cluster | ⚠️ Secrets | ✅ Governança centralizada |
# MAGIC | Granularidade | Cluster-wide | Session | Per-user (RBAC) |
# MAGIC | Recomendação | Evitar | OK para dev | ⭐ Produção |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercícios

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício A — Complete os URIs
# MAGIC
# MAGIC Preencha o URI correto para cada cloud:
# MAGIC
# MAGIC ```python
# MAGIC # Azure ADLS Gen2
# MAGIC df = spark.read.csv("____://container@account.____.windows.net/path")
# MAGIC
# MAGIC # AWS S3
# MAGIC df = spark.read.csv("____://bucket-name/path")
# MAGIC
# MAGIC # GCP GCS
# MAGIC df = spark.read.csv("____://bucket-name/path")
# MAGIC ```
# MAGIC
# MAGIC > _Respostas: abfss, dfs.core, s3, gs_

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício B — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Qual método de acesso a cloud storage é recomendado com Unity Catalog?
# MAGIC
# MAGIC - a) `dbutils.fs.mount()` com storage account key
# MAGIC - b) Acesso direto com `spark.conf.set()` e access keys
# MAGIC - c) External Locations com Storage Credentials
# MAGIC - d) Cluster-scoped credentials com Instance Profile
# MAGIC
# MAGIC > _Sua resposta: ___
# MAGIC >
# MAGIC > **Gabarito:** c) External Locations — é o método nativo do Unity Catalog com governança centralizada.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício C — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Um engenheiro precisa ler CSVs de um container Azure ADLS Gen2 chamado `raw-data`
# MAGIC na storage account `datalake01`. Qual URI está correto?
# MAGIC
# MAGIC - a) `wasbs://raw-data@datalake01.blob.core.windows.net/`
# MAGIC - b) `abfss://raw-data@datalake01.dfs.core.windows.net/`
# MAGIC - c) `adls://datalake01/raw-data/`
# MAGIC - d) `azure://datalake01.dfs.core.windows.net/raw-data/`
# MAGIC
# MAGIC > _Sua resposta: ___
# MAGIC >
# MAGIC > **Gabarito:** b) `abfss://` é o protocolo para ADLS Gen2 (Azure Blob File System Secure).

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício D — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Por que `dbutils.fs.mount()` NÃO é recomendado com Unity Catalog?
# MAGIC
# MAGIC - a) Porque é mais lento que acesso direto
# MAGIC - b) Porque não suporta formato Delta
# MAGIC - c) Porque bypassa a governança do UC — todos no cluster têm o mesmo acesso
# MAGIC - d) Porque não funciona com ADLS Gen2
# MAGIC
# MAGIC > _Sua resposta: ___
# MAGIC >
# MAGIC > **Gabarito:** c) Mount points dão acesso cluster-wide, sem controle per-user do UC.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício E — Prático
# MAGIC
# MAGIC Simule a criação de External Locations para as 3 clouds.
# MAGIC Complete o SQL:
# MAGIC
# MAGIC ```sql
# MAGIC -- Azure
# MAGIC CREATE STORAGE CREDENTIAL ___ WITH (AZURE_MANAGED_IDENTITY = '___');
# MAGIC CREATE EXTERNAL LOCATION ___ URL '___' WITH (STORAGE CREDENTIAL ___);
# MAGIC
# MAGIC -- AWS
# MAGIC CREATE STORAGE CREDENTIAL ___ WITH (AWS_IAM_ROLE = '___');
# MAGIC CREATE EXTERNAL LOCATION ___ URL '___' WITH (STORAGE CREDENTIAL ___);
# MAGIC
# MAGIC -- GCP
# MAGIC CREATE STORAGE CREDENTIAL ___ WITH (GCP_SERVICE_ACCOUNT = '___');
# MAGIC CREATE EXTERNAL LOCATION ___ URL '___' WITH (STORAGE CREDENTIAL ___);
# MAGIC ```
