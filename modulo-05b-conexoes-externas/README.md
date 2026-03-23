# Módulo 05-B — Conexões com Fontes Externas (Landing Zones)

## 🎯 Objetivo

Aprender a conectar o Databricks com **fontes externas reais**: cloud storage (Azure, AWS, GCP),
bancos relacionais (SQL Server, PostgreSQL, MySQL, Oracle) e NoSQL (MongoDB, Cosmos DB, DynamoDB).

**Relevância para o exame:** O domínio "Incremental Data Processing" e "Production Pipelines"
exige entender como dados chegam ao Lakehouse a partir de sistemas externos.

---

## 📦 Material de Apoio

- [Playlist: Aprender Databricks - Curso Completo Gratuito](https://www.youtube.com/playlist?list=PLnxUgOpvXnWzTkha0174lXfES019PQz9U)
- [Databricks Docs — External Data Sources](https://docs.databricks.com/en/connect/external-systems/index.html)
- [Databricks Docs — Unity Catalog External Locations](https://docs.databricks.com/en/sql/language-manual/sql-ref-external-locations.html)

---

## 📂 Exercícios

| # | Arquivo | Tema | Dificuldade |
|---|---------|------|-------------|
| 1 | `ex_01_cloud_storage.py` | Azure ADLS, AWS S3, GCP GCS — mount, credenciais, external locations | ⭐⭐ |
| 2 | `ex_02_bancos_relacionais.py` | JDBC — SQL Server, PostgreSQL, MySQL, Oracle | ⭐⭐ |
| 3 | `ex_03_bancos_nosql.py` | MongoDB, Cosmos DB, DynamoDB | ⭐⭐⭐ |
| 4 | `ex_04_apis_arquivos.py` | REST APIs, arquivos (CSV, JSON, Parquet) em landing zones | ⭐⭐ |
| 5 | `ex_05_secrets_seguranca.py` | Databricks Secrets, Key Vault, IAM Roles — boas práticas | ⭐⭐⭐ |
| 6 | `ex_06_landing_to_bronze.py` | Pipeline completo: landing zone → bronze (Auto Loader + JDBC) | ⭐⭐⭐ |

---

## ✅ Checkpoint do Módulo

- [ ] Cloud Storage (ADLS, S3, GCS) — montagem e acesso direto
- [ ] External Locations no Unity Catalog
- [ ] JDBC com bancos relacionais (leitura e escrita)
- [ ] Conexão com bancos NoSQL
- [ ] Secrets e segurança de credenciais
- [ ] Pipeline landing → bronze completo
- [ ] Exercícios exam-style completados
