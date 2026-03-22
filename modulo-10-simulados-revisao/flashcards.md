# Flashcards — Databricks Data Engineer Associate

Revise estes flashcards diariamente na semana anterior ao exame.
Cubra a resposta e tente responder de memória.

---

## Lakehouse Platform

**P: Qual a diferença entre Lakehouse, Data Lake e Data Warehouse?**
R: Lakehouse combina armazenamento barato do Data Lake com ACID/performance do Data Warehouse, usando Delta Lake.

**P: All-Purpose Cluster vs Job Cluster?**
R: All-Purpose: interativo, compartilhável, mais caro. Job Cluster: criado/destruído por job, mais barato para produção.

**P: O que faz o magic command `%run`?**
R: Executa outro notebook dentro do atual, compartilhando variáveis e contexto.

---

## ELT com Spark SQL e Python

**P: O que é lazy evaluation?**
R: Transformations são registradas mas não executadas até que uma Action (show, count, collect, write) seja chamada.

**P: Temp View vs Global Temp View?**
R: Temp View: visível apenas na sessão Spark. Global Temp View: visível em todas as sessões, registrada em `global_temp`.

**P: Qual opção lê CSV com cabeçalho?**
R: `.option("header", "true")`

**P: Qual formato não precisa de inferSchema?**
R: Parquet e Delta (schema embutido no arquivo).

---

## Delta Lake

**P: O que é MERGE?**
R: Operação upsert: UPDATE se existe (MATCHED), INSERT se não existe (NOT MATCHED), opcionalmente DELETE.

**P: Time Travel — como consultar versão anterior?**
R: `SELECT * FROM tabela VERSION AS OF 2` ou `TIMESTAMP AS OF '2024-01-01'`

**P: O que o OPTIMIZE faz?**
R: Compacta small files em arquivos maiores para melhor performance de leitura.

**P: O que o ZORDER faz?**
R: Co-localiza dados relacionados nos mesmos arquivos, acelerando queries que filtram por essas colunas.

**P: Qual a retenção padrão do VACUUM?**
R: 168 horas (7 dias).

**P: VACUUM afeta Time Travel?**
R: Sim. Após VACUUM, versões cujos arquivos foram removidos não podem ser consultadas.

**P: Schema Enforcement vs Schema Evolution?**
R: Enforcement: rejeita escrita com schema diferente (padrão). Evolution: `.option("mergeSchema", "true")` aceita novas colunas.

**P: Managed Table vs External Table — o que acontece no DROP?**
R: Managed: deleta dados + metadados. External: deleta apenas metadados, dados permanecem.

**P: DEEP CLONE vs SHALLOW CLONE?**
R: Deep: cópia completa (independente). Shallow: cópia de metadados (referencia dados da original).

---

## Incremental Data Processing

**P: Auto Loader usa qual formato?**
R: `spark.readStream.format("cloudFiles")`

**P: Auto Loader vs COPY INTO?**
R: Auto Loader: streaming, detecta novos arquivos automaticamente, melhor para muitos arquivos. COPY INTO: SQL, idempotente, mais simples.

**P: Qual trigger processa tudo disponível e para?**
R: `trigger(availableNow=True)`

**P: Para que serve o checkpoint?**
R: Armazena progresso do stream para retomar após falhas.

---

## Production Pipelines

**P: O que são task values em Workflows?**
R: Mecanismo para passar dados entre tasks: `dbutils.jobs.taskValues.set()` e `.get()`.

**P: expect vs expect_or_drop vs expect_or_fail?**
R: expect: registra métrica mas mantém registro. expect_or_drop: remove registro inválido. expect_or_fail: falha o pipeline.

**P: Streaming Table vs Materialized View no DLT?**
R: Streaming Table: append-only, processamento incremental. Materialized View: reprocessamento completo a cada atualização.

---

## Data Governance

**P: Hierarquia do Unity Catalog?**
R: Metastore → Catalog → Schema → Table/View/Function

**P: Referência completa de uma tabela no UC?**
R: `catalog.schema.table` (3 níveis)

**P: USAGE vs SELECT?**
R: USAGE: permite navegar/acessar o objeto. SELECT: permite ler dados. Ambos são necessários.

**P: Permissões mínimas para ler `prod.gold.revenue`?**
R: USAGE em `prod` + USAGE em `gold` + SELECT em `revenue`.

**P: O que são Dynamic Views?**
R: Views que aplicam mascaramento ou filtro de linhas baseado na identidade do usuário (`current_user()`, `is_account_group_member()`).

**P: Hive Metastore vs Unity Catalog — principal diferença?**
R: Hive: workspace-level, 2 níveis (db.table). UC: account-level (multi-workspace), 3 níveis (catalog.schema.table).

**P: Como migrar tabela Delta do Hive para UC?**
R: `CREATE TABLE uc.schema.table DEEP CLONE hive_metastore.db.table`

**P: Como migrar tabela Parquet do Hive para UC?**
R: `CREATE TABLE uc.schema.table USING DELTA AS SELECT * FROM hive_metastore.db.table` (CTAS)
