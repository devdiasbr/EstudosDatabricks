# Exam Guide Checklist — Data Engineer Associate

Mapeie cada tópico do Exam Guide oficial para os módulos do plano.
Marque como concluído quando se sentir confiante no tema.

---

## Domínio 1: Databricks Lakehouse Platform (~24%)

- [ ] Descrever a arquitetura Lakehouse → **Módulo 1**
- [ ] Diferenciar Data Lake, Warehouse e Lakehouse → **Módulo 1, Ex 1.1**
- [ ] Identificar componentes do Workspace → **Módulo 1, Ex 2**
- [ ] Criar e gerenciar clusters → **Módulo 2, Ex 2**
- [ ] Diferenciar All-Purpose vs Job Clusters → **Módulo 2, Ex 2**
- [ ] Criar e usar notebooks → **Módulo 2, Ex 3**
- [ ] Usar magic commands → **Módulo 2, Ex 3**
- [ ] Usar dbutils → **Módulo 2, Ex 1**
- [ ] Conectar repos/git ao Workspace → **Módulo 6, Ex 5**
- [ ] SQL Warehouses (Classic, Pro, Serverless) → **Módulo 5, Ex 5**
- [ ] Diferenciar SQL Warehouse vs All-Purpose Cluster → **Módulo 5, Ex 5**

---

## Domínio 2: ELT with Spark SQL and Python (~29%)

- [ ] Ler dados em múltiplos formatos (CSV, JSON, Parquet, Delta, Avro) → **Módulo 3, Ex 1 + Módulo 5-B, Ex 4**
- [ ] Usar schema explícito (StructType) → **Módulo 3, Ex 1.5**
- [ ] Aplicar transformações (select, filter, groupBy, join) → **Módulo 3, Ex 2**
- [ ] Criar e usar Temp Views → **Módulo 3, Ex 3**
- [ ] Diferenciar Actions vs Transformations → **Módulo 3**
- [ ] Escrever queries SparkSQL → **Módulo 3, Ex 3**
- [ ] Usar higher-order functions → **Módulo 3, Ex 5**
- [ ] Trabalhar com dados aninhados (JSON/structs) → **Módulo 3, Ex 5 + Módulo 5-B, Ex 3**
- [ ] Operações Delta: INSERT, UPDATE, DELETE → **Módulo 4, Ex 1**
- [ ] MERGE (upsert) → **Módulo 4, Ex 1**
- [ ] Time Travel → **Módulo 4, Ex 2**
- [ ] OPTIMIZE e Z-Ordering → **Módulo 4, Ex 3**
- [ ] VACUUM → **Módulo 4, Ex 3**
- [ ] Schema Enforcement vs Schema Evolution → **Módulo 4, Ex 4**
- [ ] Managed vs External Tables → **Módulo 4, Ex 5**
- [ ] DEEP CLONE vs SHALLOW CLONE → **Módulo 4, Ex 5**
- [ ] CTAS (CREATE TABLE AS SELECT) → **Módulo 4, Ex 5**

---

## Domínio 3: Incremental Data Processing (~16%)

- [ ] Structured Streaming conceitos → **Módulo 5, Ex 3**
- [ ] Auto Loader (cloudFiles) → **Módulo 5, Ex 3**
- [ ] COPY INTO → **Módulo 5, Ex 3**
- [ ] Trigger modes (availableNow, processingTime) → **Módulo 5, Ex 3**
- [ ] Checkpointing → **Módulo 5, Ex 3**
- [ ] Auto Loader vs COPY INTO → **Módulo 5, Ex 3**
- [ ] Conectar a fontes externas (Cloud Storage, JDBC) → **Módulo 5-B, Ex 1-2**
- [ ] CDC (Change Data Capture) conceito → **Módulo 5, Ex 4**
- [ ] APPLY CHANGES INTO (DLT CDC) → **Módulo 5, Ex 4**
- [ ] SCD Type 1 vs SCD Type 2 → **Módulo 5, Ex 4**

---

## Domínio 4: Production Pipelines (~16%)

- [ ] Criar e configurar Workflows → **Módulo 5, Ex 1**
- [ ] Task dependencies (DAG) → **Módulo 5, Ex 1**
- [ ] Task values → **Módulo 5, Ex 1**
- [ ] Job Cluster vs All-Purpose → **Módulo 2 + Módulo 5**
- [ ] Delta Live Tables (DLT) → **Módulo 5, Ex 2**
- [ ] Expectations (expect, expect_or_drop, expect_or_fail) → **Módulo 5, Ex 2**
- [ ] Arquitetura Medallion → **Módulo 3, Ex 4 + Módulo 5, Ex 2**
- [ ] Streaming Tables vs Materialized Views → **Módulo 5, Ex 2**
- [ ] Dashboards e Alerts (DBSQL) → **Módulo 5, Ex 5**
- [ ] Databricks Repos e CI/CD → **Módulo 6, Ex 5**

---

## Domínio 5: Data Governance (~11%)

- [ ] Unity Catalog: hierarquia e propósito → **Módulo 6, Ex 1**
- [ ] Referência de 3 níveis (catalog.schema.table) → **Módulo 6, Ex 1**
- [ ] GRANT/REVOKE permissões → **Módulo 6, Ex 2**
- [ ] USAGE vs SELECT → **Módulo 6, Ex 2**
- [ ] Dynamic Views (mascaramento) → **Módulo 6, Ex 3**
- [ ] Delta Sharing → **Módulo 6, Ex 4**
- [ ] Data Lineage (table, column, notebook-level) → **Módulo 6, Ex 5**
- [ ] External Locations e Storage Credentials → **Módulo 5-B, Ex 1**
- [ ] Databricks Secrets (segurança de credenciais) → **Módulo 5-B, Ex 5**
- [ ] Hive Metastore (legado) → **Módulo 7**
- [ ] Migração Hive → UC → **Módulo 8 (bônus)**

---

## Contagem de Progresso

| Domínio | Total | Concluídos | % |
|---------|-------|-----------|---|
| Lakehouse Platform | 11 | ___ | ___% |
| ELT Spark SQL/Python | 17 | ___ | ___% |
| Incremental Data | 10 | ___ | ___% |
| Production Pipelines | 10 | ___ | ___% |
| Data Governance | 11 | ___ | ___% |
| **TOTAL** | **59** | ___ | ___% |

> **Meta:** 100% concluído antes de agendar o exame.
> **Dica:** Use o Exam Guide oficial da Databricks para validar que não faltou nada:
> https://www.databricks.com/learn/certification/data-engineer-associate
