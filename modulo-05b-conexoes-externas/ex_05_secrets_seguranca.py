# Databricks notebook source

# MAGIC %md
# MAGIC # Exercício 5 — Secrets, Segurança e Boas Práticas
# MAGIC
# MAGIC **Módulo 05-B — Conexões com Fontes Externas**
# MAGIC
# MAGIC **Objetivo:** Gerenciar credenciais de forma segura com Databricks Secrets,
# MAGIC integrar com Azure Key Vault / AWS Secrets Manager / GCP Secret Manager.
# MAGIC **Tema cobrado no exame!**

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.1 — Databricks Secrets
# MAGIC
# MAGIC ### O que são?
# MAGIC - Armazenamento seguro de credenciais (senhas, tokens, keys)
# MAGIC - Nunca aparecem em logs ou outputs (são redactados: `[REDACTED]`)
# MAGIC - Organizados em **Scopes** (agrupamentos lógicos)
# MAGIC - Acessados via `dbutils.secrets.get(scope, key)`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Criando Secrets via CLI

# COMMAND ----------

# --- Databricks CLI: Gerenciamento de Secrets ---

# Criar um scope
# databricks secrets create-scope --scope meu-scope

# Adicionar um secret
# databricks secrets put --scope meu-scope --key minha-senha

# Listar scopes
# databricks secrets list-scopes

# Listar secrets de um scope
# databricks secrets list --scope meu-scope

# Deletar
# databricks secrets delete --scope meu-scope --key minha-senha

print("Secrets CLI — comandos de gerenciamento")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Usando Secrets no código

# COMMAND ----------

# --- Usando Secrets ---

# Leitura de um secret
# password = dbutils.secrets.get(scope="jdbc", key="sqlserver-pass")
# print(password)  # Saída: [REDACTED]

# Uso em conexão JDBC
# jdbc_url = "jdbc:sqlserver://meu-server:1433;database=mydb"
# df = spark.read.jdbc(
#     url=jdbc_url,
#     table="vendas",
#     properties={
#         "user": dbutils.secrets.get(scope="jdbc", key="sqlserver-user"),
#         "password": dbutils.secrets.get(scope="jdbc", key="sqlserver-pass"),
#         "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
#     }
# )

# Uso em API REST
# import requests
# headers = {
#     "Authorization": f"Bearer {dbutils.secrets.get(scope='api', key='token')}"
# }

# Listar secrets disponíveis
# dbutils.secrets.listScopes()
# dbutils.secrets.list(scope="jdbc")

print("Secrets — uso seguro no código")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5.2 — Integração com Key Vaults

# COMMAND ----------

# MAGIC %md
# MAGIC ### Azure Key Vault-backed Scope
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────┐     Secret Scope      ┌──────────────────────┐
# MAGIC │  Databricks  │ ◄──────────────────── │  Azure Key Vault     │
# MAGIC │  Notebook    │   dbutils.secrets.get  │  (secrets centrais)  │
# MAGIC └──────────────┘                        └──────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC **Configuração:**
# MAGIC 1. Criar Key Vault no Azure Portal
# MAGIC 2. Adicionar secrets no Key Vault
# MAGIC 3. Criar scope no Databricks apontando para o Key Vault:
# MAGIC    - URL: `https://<workspace-url>#secrets/createScope`
# MAGIC    - Informar: Scope Name, Key Vault DNS, Resource ID
# MAGIC 4. Usar: `dbutils.secrets.get(scope="kv-scope", key="minha-key")`

# COMMAND ----------

# MAGIC %md
# MAGIC ### AWS Secrets Manager
# MAGIC
# MAGIC ```python
# MAGIC # Opção 1: Databricks Secret Scope (recomendado)
# MAGIC # databricks secrets create-scope --scope aws-secrets --scope-backend-type DATABRICKS
# MAGIC
# MAGIC # Opção 2: Boto3 direto (para cenários específicos)
# MAGIC # import boto3
# MAGIC # client = boto3.client("secretsmanager", region_name="us-east-1")
# MAGIC # secret = client.get_secret_value(SecretId="meu-secret")
# MAGIC # credentials = json.loads(secret["SecretString"])
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### GCP Secret Manager
# MAGIC
# MAGIC ```python
# MAGIC # from google.cloud import secretmanager
# MAGIC # client = secretmanager.SecretManagerServiceClient()
# MAGIC # name = "projects/meu-projeto/secrets/meu-secret/versions/latest"
# MAGIC # response = client.access_secret_version(request={"name": name})
# MAGIC # secret_value = response.payload.data.decode("UTF-8")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5.3 — Boas Práticas de Segurança
# MAGIC
# MAGIC | ❌ NÃO FAÇA | ✅ FAÇA |
# MAGIC |-------------|---------|
# MAGIC | Hardcode de senhas no notebook | Use `dbutils.secrets.get()` |
# MAGIC | Imprimir secrets no output | Secrets são redactados automaticamente |
# MAGIC | Compartilhar secrets via chat/email | Use Key Vault com RBAC |
# MAGIC | Usar access keys da storage account | Use Managed Identity + External Location |
# MAGIC | Dar permissão de owner em tudo | Princípio do menor privilégio |
# MAGIC | Guardar .env no repositório Git | Use `.gitignore` + Secrets |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5.4 — Controle de Acesso: Quem acessa o quê?
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────┐
# MAGIC │           CAMADAS DE SEGURANÇA                   │
# MAGIC ├─────────────────────────────────────────────────┤
# MAGIC │ 1. Identity Provider (Azure AD / Okta / SSO)    │
# MAGIC │ 2. Workspace Access Control (admin/user)        │
# MAGIC │ 3. Cluster Policies (quem cria/usa clusters)    │
# MAGIC │ 4. Secret Scopes (ACLs por scope)               │
# MAGIC │ 5. Unity Catalog (GRANT/REVOKE em tabelas)      │
# MAGIC │ 6. External Locations (acesso a storage)        │
# MAGIC │ 7. Row/Column Level Security (Dynamic Views)    │
# MAGIC └─────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# --- ACLs em Secret Scopes ---

# Dar acesso de leitura a um grupo
# databricks secrets put-acl --scope jdbc --principal data-engineers --permission READ

# Listar ACLs
# databricks secrets list-acls --scope jdbc

# Permissões possíveis: READ, WRITE, MANAGE

print("Secret Scope ACLs — controle de acesso granular")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercícios

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício A — Exam-Style
# MAGIC
# MAGIC **Pergunta:** O que acontece quando você tenta imprimir um Databricks secret no output de um notebook?
# MAGIC
# MAGIC - a) O valor é exibido normalmente
# MAGIC - b) O notebook retorna um erro
# MAGIC - c) O valor é substituído por `[REDACTED]`
# MAGIC - d) O cluster é reiniciado por segurança
# MAGIC
# MAGIC > _Sua resposta: ___
# MAGIC >
# MAGIC > **Gabarito:** c) Databricks automaticamente redacta secrets nos outputs.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício B — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Qual é a forma MAIS segura de conectar Azure ADLS Gen2 ao Databricks com Unity Catalog?
# MAGIC
# MAGIC - a) Storage account key via `spark.conf.set()`
# MAGIC - b) SAS Token no notebook
# MAGIC - c) Service Principal key no Databricks Secrets
# MAGIC - d) Azure Managed Identity via Storage Credential + External Location
# MAGIC
# MAGIC > _Sua resposta: ___
# MAGIC >
# MAGIC > **Gabarito:** d) Managed Identity + External Location = zero secrets no código.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício C — Exam-Style
# MAGIC
# MAGIC **Pergunta:** Qual comando lista todos os secret scopes disponíveis?
# MAGIC
# MAGIC - a) `dbutils.secrets.list()`
# MAGIC - b) `dbutils.secrets.listScopes()`
# MAGIC - c) `dbutils.secrets.getScopes()`
# MAGIC - d) `spark.conf.get("secrets.scopes")`
# MAGIC
# MAGIC > _Sua resposta: ___
# MAGIC >
# MAGIC > **Gabarito:** b) `dbutils.secrets.listScopes()` lista os scopes; `dbutils.secrets.list(scope)` lista as keys.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício D — Prático
# MAGIC
# MAGIC Ordene as etapas para configurar um Azure Key Vault-backed scope:
# MAGIC
# MAGIC - [ ] Criar Key Vault no Azure Portal
# MAGIC - [ ] Adicionar secrets no Key Vault
# MAGIC - [ ] Acessar `https://<workspace>#secrets/createScope`
# MAGIC - [ ] Informar Scope Name, DNS Name, Resource ID
# MAGIC - [ ] Usar `dbutils.secrets.get(scope="kv-scope", key="minha-key")`
# MAGIC
# MAGIC > _Numere de 1 a 5_
