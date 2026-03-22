# Plano de Estudos — Databricks Certified Data Engineer Associate

**Baseado na playlist:** [Aprender Databricks - Curso Completo Gratuito](https://www.youtube.com/playlist?list=PLnxUgOpvXnWzTkha0174lXfES019PQz9U)
**Certificação alvo:** Databricks Certified Data Engineer Associate
**Total de vídeos:** 15 | **Duração do curso:** ~2h05min
**Data de início:** ___/___/______

---

## Sobre o Exame — Data Engineer Associate

| Item | Detalhe |
|------|---------|
| Formato | 45 questões de múltipla escolha |
| Duração | 90 minutos |
| Nota mínima | 70% |
| Idioma | Inglês |
| Custo | USD 200 |
| Domínios | Databricks Lakehouse Platform, ELT com Spark SQL e PySpark, Incremental Data Processing, Production Pipelines, Data Governance |

---

## Visão Geral do Plano

| Semana | Módulo | Tema Principal | Vídeos | Peso no Exame |
|--------|--------|---------------|--------|---------------|
| 1 | Módulo 1 | Fundamentos do Databricks & Lakehouse | 1–3 | ~24% |
| 1 | Módulo 2 | Ambiente Prático — Clusters e Notebooks | 4–6 | ~24% |
| 2 | Módulo 3 | ELT com PySpark e SparkSQL | 7–9 | ~29% |
| 2 | Módulo 4 | Delta Lake Profundo | 8 (revisão) | ~29% |
| 3 | Módulo 5 | Pipelines de Produção (Workflows + DLT) | 10–11 | ~16% |
| 3 | Módulo 6 | Unity Catalog & Governança de Dados | 12 | ~11% |
| 4 | Módulo 7 | Hive Metastore — Legado e Fundamentos | — | ~11% |
| 4 | Módulo 8 | BÔNUS: Migração Hive → Unity Catalog | — | Diferencial |
| 5 | Módulo 9 | Certificação, Mindset e Preparação Final | 13–15 | — |
| 5–6 | Módulo 10 | Simulados e Revisão Final | — | — |

---

## Módulo 1 — Fundamentos do Databricks & Lakehouse Platform
**Objetivo:** Entender a plataforma, a arquitetura Lakehouse e a estrutura do Workspace.
**Domínio do exame:** Databricks Lakehouse Platform (~24%)

### Vídeo 1 — Boas Vindas (3:24)
- [ ] Assistir ao vídeo
- [ ] Anotar os objetivos gerais do curso
- [ ] Entender o roadmap até a certificação

### Vídeo 2 — O que é Databricks? (8:28)
- [ ] Assistir ao vídeo
- [ ] **Conceitos-chave para anotar:**
  - O que é a Lakehouse Platform
  - Diferença entre Data Lake, Data Warehouse e Lakehouse
  - Integração com Apache Spark
  - Casos de uso: Data Engineering, Data Science, ML, Analytics
  - O papel do Data Engineer no ecossistema Databricks

#### Exercícios — Vídeo 2
- [ ] **Ex 2.1:** Escrever um resumo de 5 linhas explicando o que é Databricks com suas palavras
- [ ] **Ex 2.2:** Criar uma tabela comparativa entre Data Lake, Data Warehouse e Lakehouse com as colunas: Armazenamento, Performance, ACID, Schema, Casos de uso
- [ ] **Ex 2.3:** Listar 5 problemas que o modelo Lakehouse resolve em relação ao Data Lake tradicional
- [ ] **Ex 2.4 (Exam-style):** Responder — "Uma empresa precisa de analytics em tempo real com dados estruturados e não-estruturados, além de suporte a ML. Qual arquitetura é mais adequada e por quê?"

### Vídeo 3 — Workspace (7:41)
- [ ] Assistir ao vídeo
- [ ] **Conceitos-chave para anotar:**
  - Estrutura do Workspace (pastas, notebooks, repos)
  - Navegação na interface
  - Organização de projetos dentro do Workspace
  - Relação entre Workspace e controle de acesso

#### Exercícios — Vídeo 3
- [ ] **Ex 3.1:** Criar um mapa mental da estrutura do Workspace
- [ ] **Ex 3.2:** Listar todos os componentes acessíveis pela barra lateral do Databricks e a função de cada um
- [ ] **Ex 3.3 (Exam-style):** "Qual é o local recomendado para armazenar notebooks compartilhados entre membros de um time no Databricks?"

---

## Módulo 2 — Ambiente Prático — Clusters e Notebooks
**Objetivo:** Dominar a configuração do ambiente de trabalho do Data Engineer.
**Domínio do exame:** Databricks Lakehouse Platform (~24%)

### Vídeo 4 — Workspace na Prática - Data Engineering (12:17)
- [ ] Assistir ao vídeo
- [ ] **Conceitos-chave:** Fluxo de trabalho do Data Engineer no Databricks

#### Exercícios — Vídeo 4
- [ ] **Ex 4.1 (Hands-on):** Acessar o Databricks Community Edition ou Databricks Free
- [ ] **Ex 4.2 (Hands-on):** Criar a seguinte estrutura de pastas: `/Users/seu-email/certificacao/bronze`, `/silver`, `/gold`
- [ ] **Ex 4.3 (Hands-on):** Importar um notebook de exemplo da Databricks Academy
- [ ] **Ex 4.4 (Hands-on):** Criar um notebook novo chamado `setup_ambiente` e escrever células que:
  - Listam os databases existentes (`SHOW DATABASES`)
  - Verificam o cluster runtime (`spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion")`)
  - Listam os arquivos em `/databricks-datasets/`
- [ ] **Ex 4.5:** Documentar o passo a passo para configurar um ambiente de Data Engineering do zero

### Vídeo 5 — Clusters (4:57)
- [ ] Assistir ao vídeo
- [ ] **Conceitos-chave para anotar:**
  - Tipos de clusters (All-Purpose vs Job Clusters)
  - Configurações principais (autoscaling, Databricks Runtime, node types)
  - Ciclo de vida do cluster (criação, início, auto-terminação)
  - Cluster policies

#### Exercícios — Vídeo 5
- [ ] **Ex 5.1 (Hands-on):** Criar um cluster All-Purpose com autoscaling de 1–2 workers e auto-terminação em 30min
- [ ] **Ex 5.2:** Criar tabela comparativa: All-Purpose Cluster vs Job Cluster (custo, uso, quem cria, quando usar)
- [ ] **Ex 5.3 (Exam-style):** "Um pipeline de produção roda diariamente às 02:00. Que tipo de cluster deve ser usado e por quê?"
- [ ] **Ex 5.4 (Exam-style):** "Qual configuração de cluster reduz custos quando a carga de trabalho é variável?"
- [ ] **Ex 5.5:** Explicar o que acontece com os dados em memória quando um cluster é terminado vs reiniciado

### Vídeo 6 — Cadernos do Databricks (3:17)
- [ ] Assistir ao vídeo
- [ ] **Conceitos-chave para anotar:**
  - Linguagens suportadas (Python, SQL, Scala, R)
  - Magic commands (`%sql`, `%python`, `%md`, `%run`, `%fs`)
  - Execução de células e visualização de resultados
  - Widgets e parâmetros em notebooks

#### Exercícios — Vídeo 6
- [ ] **Ex 6.1 (Hands-on):** Criar um notebook multilíngue com:
  - Célula 1: `%md` com título e descrição do exercício
  - Célula 2: `%python` — criar um DataFrame com 5 linhas
  - Célula 3: `%sql` — consultar o DataFrame como temp view
  - Célula 4: `%fs` — listar arquivos em `/databricks-datasets/`
- [ ] **Ex 6.2 (Hands-on):** Usar `%run` para executar o notebook `setup_ambiente` dentro de outro notebook
- [ ] **Ex 6.3 (Exam-style):** "Qual magic command permite executar um notebook dentro de outro e compartilhar variáveis?"
- [ ] **Ex 6.4:** Criar um notebook com widgets (`dbutils.widgets`) que recebe um parâmetro de data e filtra dados com base nele

---

## Módulo 3 — ELT com PySpark e SparkSQL
**Objetivo:** Dominar leitura, transformação e escrita de dados — o coração do exame.
**Domínio do exame:** ELT with Spark SQL and Python (~29%)

### Vídeo 7 — Processando dados com PySpark e SparkSQL (8:19)
- [ ] Assistir ao vídeo
- [ ] **Conceitos-chave para anotar:**
  - Leitura de dados (CSV, JSON, Parquet, Delta)
  - Opções de leitura (`header`, `inferSchema`, `multiLine`, `sep`)
  - Transformações com DataFrames (select, filter, withColumn, groupBy, join, orderBy)
  - Diferença entre PySpark e SparkSQL
  - Lazy evaluation — Actions vs Transformations
  - Temp Views e Global Temp Views

#### Exercícios — Vídeo 7
- [ ] **Ex 7.1 (Hands-on):** Carregar o dataset `/databricks-datasets/learning-spark-v2/people/people-10m.delta` e executar:
  ```python
  # PySpark
  df = spark.read.format("delta").load("/databricks-datasets/learning-spark-v2/people/people-10m.delta")
  df.filter(df.salary > 50000).groupBy("gender").count().show()
  ```
- [ ] **Ex 7.2 (Hands-on):** Criar uma Temp View e realizar a mesma consulta com SparkSQL:
  ```sql
  CREATE OR REPLACE TEMP VIEW people AS
  SELECT * FROM delta.`/databricks-datasets/learning-spark-v2/people/people-10m.delta`;
  SELECT gender, count(*) FROM people WHERE salary > 50000 GROUP BY gender;
  ```
- [ ] **Ex 7.3 (Hands-on):** Praticar JOINs — criar 2 DataFrames (clientes e pedidos) e realizar INNER, LEFT e ANTI JOIN
- [ ] **Ex 7.4:** Criar tabela comparativa PySpark vs SparkSQL para cada operação: select, filter, groupBy, join, orderBy
- [ ] **Ex 7.5 (Hands-on):** Usar `withColumn` + funções do `pyspark.sql.functions` (col, lit, when, regexp_replace, date_format) para transformar pelo menos 3 colunas
- [ ] **Ex 7.6 (Exam-style):** "Qual a diferença entre uma Temp View e uma Global Temp View? Em qual database a Global Temp View é registrada?"
- [ ] **Ex 7.7 (Exam-style):** "O que significa lazy evaluation no Spark? Dê um exemplo de transformation e um de action."
- [ ] **Ex 7.8 (Hands-on):** Ler um arquivo CSV sem schema e depois com schema explícito usando `StructType`. Comparar os tipos inferidos vs definidos.

### Vídeo 9 — Processamento e Análise de Dados (9:24)
- [ ] Assistir ao vídeo
- [ ] **Conceitos-chave para anotar:**
  - Técnicas de análise exploratória no Databricks
  - Visualizações nativas do notebook
  - Funções de agregação avançadas
  - Boas práticas de processamento

#### Exercícios — Vídeo 9
- [ ] **Ex 9.1 (Hands-on):** Análise exploratória completa de um dataset:
  - Contar registros totais e nulos por coluna
  - Calcular estatísticas descritivas (`describe()`, `summary()`)
  - Identificar duplicatas
  - Criar pelo menos 3 visualizações nativas
- [ ] **Ex 9.2 (Hands-on):** Implementar um pipeline ELT completo em notebook:
  1. **Extract:** Ler CSV bruto de `/databricks-datasets/`
  2. **Load:** Salvar como tabela Delta na camada bronze
  3. **Transform:** Limpar dados (remover nulos, padronizar tipos) e salvar na camada silver
  4. **Aggregate:** Criar tabela gold com métricas agregadas
- [ ] **Ex 9.3 (Hands-on):** Praticar higher-order functions com arrays:
  ```sql
  SELECT transform(array(1,2,3), x -> x * 2);
  SELECT filter(array(1,2,3,4,5), x -> x > 3);
  SELECT exists(array(1,2,3), x -> x = 2);
  ```
- [ ] **Ex 9.4 (Exam-style):** "Qual função SQL do Spark é usada para explodir um array em múltiplas linhas?"
- [ ] **Ex 9.5 (Hands-on):** Usar `pivot()` para transformar linhas em colunas e `unpivot`/`stack` para o inverso

---

## Módulo 4 — Delta Lake Profundo
**Objetivo:** Dominar Delta Lake — o formato central do Lakehouse e tema recorrente no exame.
**Domínio do exame:** ELT with Spark SQL and Python (~29%) + Incremental Data Processing

### Vídeo 8 — Delta Lake e Databases do Databricks (11:42) — Revisão aprofundada
- [ ] Assistir ao vídeo (ou reassistir com foco nos detalhes)
- [ ] **Conceitos-chave para anotar:**
  - Formato Delta (Parquet + Transaction Log)
  - ACID Transactions no Data Lake
  - Time Travel (versionamento de dados)
  - Schema Enforcement vs Schema Evolution
  - `OPTIMIZE` e `VACUUM`
  - `DESCRIBE HISTORY` e `DESCRIBE DETAIL`
  - Managed Tables vs External Tables
  - `CTAS` (CREATE TABLE AS SELECT)

#### Exercícios — Delta Lake
- [ ] **Ex 8.1 (Hands-on):** Criar uma tabela Delta e executar o ciclo completo:
  ```sql
  CREATE TABLE certificacao.bronze.vendas (
    id INT, produto STRING, valor DOUBLE, data DATE
  ) USING DELTA;

  INSERT INTO certificacao.bronze.vendas VALUES
    (1, 'Notebook', 3500.00, '2024-01-15'),
    (2, 'Mouse', 89.90, '2024-01-16');

  UPDATE certificacao.bronze.vendas SET valor = 3299.00 WHERE id = 1;
  DELETE FROM certificacao.bronze.vendas WHERE id = 2;
  ```
- [ ] **Ex 8.2 (Hands-on):** Praticar Time Travel:
  ```sql
  DESCRIBE HISTORY certificacao.bronze.vendas;
  SELECT * FROM certificacao.bronze.vendas VERSION AS OF 0;
  SELECT * FROM certificacao.bronze.vendas TIMESTAMP AS OF '2024-01-15T00:00:00';
  RESTORE TABLE certificacao.bronze.vendas TO VERSION AS OF 1;
  ```
- [ ] **Ex 8.3 (Hands-on):** Praticar MERGE (upsert) — muito cobrado no exame:
  ```sql
  MERGE INTO certificacao.silver.produtos AS target
  USING certificacao.bronze.produtos_raw AS source
  ON target.id = source.id
  WHEN MATCHED THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *;
  ```
- [ ] **Ex 8.4 (Hands-on):** Executar `OPTIMIZE` com Z-Ordering:
  ```sql
  OPTIMIZE certificacao.silver.produtos ZORDER BY (categoria);
  ```
- [ ] **Ex 8.5 (Hands-on):** Executar `VACUUM` e entender a retenção:
  ```sql
  VACUUM certificacao.bronze.vendas RETAIN 168 HOURS;
  -- Tentar VACUUM com 0 HOURS e observar o erro
  ```
- [ ] **Ex 8.6 (Exam-style):** "Após executar VACUUM com retenção de 7 dias, ainda é possível fazer Time Travel para uma versão de 10 dias atrás? Por quê?"
- [ ] **Ex 8.7 (Exam-style):** "Qual a diferença entre Schema Enforcement e Schema Evolution? Como habilitar Schema Evolution?"
- [ ] **Ex 8.8 (Hands-on):** Testar Schema Evolution:
  ```python
  df_novo = spark.createDataFrame([(3, "Teclado", 199.90, "2024-02-01", "Periféricos")],
    ["id", "produto", "valor", "data", "categoria"])
  df_novo.write.format("delta").mode("append").option("mergeSchema", "true") \
    .saveAsTable("certificacao.bronze.vendas")
  ```
- [ ] **Ex 8.9 (Exam-style):** "Qual a diferença entre Managed Table e External Table? O que acontece com os dados ao executar DROP TABLE em cada uma?"
- [ ] **Ex 8.10 (Hands-on):** Criar uma tabela com CTAS e com DEEP CLONE / SHALLOW CLONE:
  ```sql
  CREATE TABLE silver.vendas_backup DEEP CLONE bronze.vendas;
  CREATE TABLE silver.vendas_dev SHALLOW CLONE bronze.vendas;
  ```

---

## Módulo 5 — Pipelines de Produção (Workflows + DLT)
**Objetivo:** Entender automação de pipelines e Delta Live Tables.
**Domínio do exame:** Production Pipelines (~16%)

### Vídeo 10 — Automação de Pipelines com Workflows (9:17)
- [ ] Assistir ao vídeo
- [ ] **Conceitos-chave para anotar:**
  - Workflows/Jobs no Databricks
  - Agendamento de tarefas (cron scheduling)
  - Dependências entre tasks (DAG)
  - Monitoramento, alertas e retries
  - Parâmetros de job e task values

#### Exercícios — Vídeo 10
- [ ] **Ex 10.1 (Hands-on):** Criar um Workflow com 3 tasks:
  - Task 1: Ingestão (notebook que lê CSV e salva em Delta bronze)
  - Task 2: Transformação (notebook que lê bronze e salva em silver) — depende da Task 1
  - Task 3: Agregação (notebook que lê silver e salva em gold) — depende da Task 2
- [ ] **Ex 10.2 (Hands-on):** Configurar o Workflow com agendamento diário e alerta por e-mail em caso de falha
- [ ] **Ex 10.3:** Desenhar o DAG do pipeline acima e anotar qual tipo de cluster cada task deveria usar
- [ ] **Ex 10.4 (Exam-style):** "Qual a vantagem de usar Job Clusters em vez de All-Purpose Clusters para Workflows de produção?"
- [ ] **Ex 10.5 (Exam-style):** "Como passar parâmetros entre tasks em um Workflow? O que são task values?"
- [ ] **Ex 10.6 (Hands-on):** Usar `dbutils.jobs.taskValues.set()` e `dbutils.jobs.taskValues.get()` para passar dados entre tasks

### Vídeo 11 — Delta Live Tables (4:40)
- [ ] Assistir ao vídeo
- [ ] **Conceitos-chave para anotar:**
  - O que são Delta Live Tables (DLT)
  - Diferença entre DLT e pipelines tradicionais
  - Declarative pipelines vs imperative
  - Expectations (regras de qualidade de dados)
  - Arquitetura Medallion (Bronze → Silver → Gold)
  - Tipos de tabelas DLT: Streaming Tables vs Materialized Views

#### Exercícios — Vídeo 11
- [ ] **Ex 11.1:** Desenhar a arquitetura Medallion completa com exemplos de dados em cada camada
- [ ] **Ex 11.2:** Escrever pseudocódigo de um pipeline DLT com 3 camadas:
  ```python
  # Bronze
  @dlt.table
  def bronze_vendas():
      return spark.readStream.format("cloudFiles") \
          .option("cloudFiles.format", "csv") \
          .load("/data/vendas/")

  # Silver
  @dlt.table
  @dlt.expect_or_drop("valor_valido", "valor > 0")
  def silver_vendas():
      return dlt.read_stream("bronze_vendas") \
          .filter(col("produto").isNotNull())

  # Gold
  @dlt.table
  def gold_vendas_diarias():
      return dlt.read("silver_vendas") \
          .groupBy("data").agg(sum("valor").alias("total"))
  ```
- [ ] **Ex 11.3 (Exam-style):** "Qual a diferença entre `@dlt.table` e `@dlt.view`?"
- [ ] **Ex 11.4 (Exam-style):** "Qual a diferença entre `expect`, `expect_or_drop` e `expect_or_fail`? Quando usar cada um?"
- [ ] **Ex 11.5 (Exam-style):** "O que é Auto Loader (`cloudFiles`) e qual sua vantagem sobre `spark.readStream.format('csv')`?"
- [ ] **Ex 11.6:** Criar tabela comparativa: Streaming Table vs Materialized View no DLT

---

## Módulo 6 — Unity Catalog & Governança de Dados
**Objetivo:** Dominar Unity Catalog — governança, controle de acesso e data lineage.
**Domínio do exame:** Data Governance (~11%)

### Vídeo 12 — Governança de Dados no Databricks (7:21)
- [ ] Assistir ao vídeo
- [ ] **Conceitos-chave para anotar:**
  - Unity Catalog: definição e propósito
  - Hierarquia de 3 níveis: Catalog → Schema → Table/View/Function
  - Metastore (nível da conta)
  - Controle de acesso (GRANT/REVOKE)
  - Data Lineage (rastreabilidade automática)
  - Auditoria e compliance

### Estudo Complementar — Unity Catalog Aprofundado

#### 6.1 — Arquitetura do Unity Catalog
- [ ] **Estudar:** Hierarquia completa: Account → Metastore → Catalog → Schema → Table
- [ ] **Conceitos adicionais:**
  - Storage Credentials e External Locations
  - Securables e Privileges
  - Identity Federation (usuários e grupos)
  - Databricks-managed vs External storage

#### Exercícios — Unity Catalog
- [ ] **Ex 6.1 (Hands-on):** Criar a estrutura completa:
  ```sql
  -- Criar catalog
  CREATE CATALOG IF NOT EXISTS empresa_dev;
  USE CATALOG empresa_dev;

  -- Criar schemas por camada
  CREATE SCHEMA IF NOT EXISTS bronze;
  CREATE SCHEMA IF NOT EXISTS silver;
  CREATE SCHEMA IF NOT EXISTS gold;

  -- Criar tabela dentro da estrutura
  CREATE TABLE empresa_dev.bronze.clientes (
    id INT, nome STRING, email STRING, criado_em TIMESTAMP
  );

  -- Verificar hierarquia
  SHOW CATALOGS;
  SHOW SCHEMAS IN empresa_dev;
  SHOW TABLES IN empresa_dev.bronze;
  ```
- [ ] **Ex 6.2 (Hands-on):** Praticar controle de acesso:
  ```sql
  -- Conceder permissões
  GRANT USAGE ON CATALOG empresa_dev TO `data_engineers`;
  GRANT SELECT ON SCHEMA empresa_dev.gold TO `analysts`;
  GRANT ALL PRIVILEGES ON SCHEMA empresa_dev.bronze TO `data_engineers`;

  -- Revogar permissões
  REVOKE SELECT ON TABLE empresa_dev.gold.vendas FROM `estagiarios`;

  -- Verificar permissões
  SHOW GRANTS ON SCHEMA empresa_dev.bronze;
  ```
- [ ] **Ex 6.3:** Criar diagrama completo da hierarquia Unity Catalog com exemplos reais (empresa → departamentos → tabelas)
- [ ] **Ex 6.4 (Exam-style):** "Qual a diferença entre USAGE e SELECT como permissão no Unity Catalog?"
- [ ] **Ex 6.5 (Exam-style):** "Um analista precisa ler dados da tabela `prod.gold.revenue`. Quais permissões mínimas são necessárias?"
  - Resposta esperada: USAGE no catalog `prod`, USAGE no schema `gold`, SELECT na tabela `revenue`
- [ ] **Ex 6.6 (Exam-style):** "O que é Data Lineage no Unity Catalog e como ele é populado?"
- [ ] **Ex 6.7:** Listar as diferenças entre Table ACLs (legado) e Unity Catalog para controle de acesso
- [ ] **Ex 6.8 (Exam-style):** "O que são Dynamic Views no Unity Catalog e para que servem?" (ex: mascaramento de dados com `current_user()` e `is_member()`)

---

## Módulo 7 — Hive Metastore — Legado e Fundamentos
**Objetivo:** Entender o Hive Metastore, sua arquitetura e limitações — base para entender a migração para Unity Catalog.

### 7.1 — O que é o Hive Metastore
- [ ] **Estudar os conceitos:**
  - Hive Metastore como catálogo de metadados padrão do Apache Hive/Spark
  - Hierarquia: Database (Schema) → Table → Partition
  - Diferença: Hive não tem o nível "Catalog" (namespace único)
  - Metastore local vs externo (MySQL, PostgreSQL)
  - Managed Tables vs External Tables no Hive

#### Exercícios — Hive Metastore
- [ ] **Ex 7.1 (Hands-on):** Explorar o Hive Metastore legado:
  ```sql
  -- Listar databases no hive_metastore
  SHOW DATABASES IN hive_metastore;

  -- Criar database legado
  CREATE DATABASE IF NOT EXISTS hive_metastore.vendas_legado;

  -- Criar tabela no formato Hive tradicional
  CREATE TABLE hive_metastore.vendas_legado.transacoes (
    id INT, produto STRING, valor DOUBLE, data STRING
  ) USING DELTA;

  -- Verificar localização
  DESCRIBE DETAIL hive_metastore.vendas_legado.transacoes;
  ```
- [ ] **Ex 7.2:** Criar tabela comparativa Hive Metastore vs Unity Catalog:

  | Característica | Hive Metastore | Unity Catalog |
  |---------------|----------------|---------------|
  | Hierarquia | Database → Table | Catalog → Schema → Table |
  | Escopo | Workspace | Account (multi-workspace) |
  | Controle de acesso | Table ACLs (limitado) | GRANT/REVOKE granular |
  | Data Lineage | Não nativo | Automático |
  | Auditoria | Manual | Nativa |
  | Storage | DBFS (workspace-level) | External Locations (account-level) |

- [ ] **Ex 7.3 (Exam-style):** "Por que o Hive Metastore é considerado legado no Databricks? Cite 3 limitações."
- [ ] **Ex 7.4 (Exam-style):** "Qual a principal diferença de escopo entre Hive Metastore e Unity Catalog?"
- [ ] **Ex 7.5:** Explicar o que é DBFS (Databricks File System) e por que não é recomendado para produção com Unity Catalog

### 7.2 — Tabelas e Formatos no Hive
- [ ] **Conceitos:**
  - SerDe (Serializer/Deserializer)
  - Formatos: Parquet, ORC, Avro, CSV, JSON
  - Particionamento no Hive (`PARTITIONED BY`)
  - Bucketing
  - Diferença entre tabela Hive particionada e Delta Lake

#### Exercícios — Formatos e Partições
- [ ] **Ex 7.6 (Hands-on):** Criar tabelas em diferentes formatos e comparar:
  ```sql
  -- Tabela Parquet
  CREATE TABLE hive_metastore.vendas_legado.parquet_table
  USING PARQUET AS SELECT * FROM certificacao.bronze.vendas;

  -- Tabela Delta
  CREATE TABLE hive_metastore.vendas_legado.delta_table
  USING DELTA AS SELECT * FROM certificacao.bronze.vendas;

  -- Comparar com DESCRIBE DETAIL
  DESCRIBE DETAIL hive_metastore.vendas_legado.parquet_table;
  DESCRIBE DETAIL hive_metastore.vendas_legado.delta_table;
  ```
- [ ] **Ex 7.7 (Hands-on):** Criar tabela particionada e testar partition pruning:
  ```sql
  CREATE TABLE hive_metastore.vendas_legado.vendas_part
  USING DELTA PARTITIONED BY (ano, mes)
  AS SELECT *, year(data) as ano, month(data) as mes
  FROM certificacao.bronze.vendas;

  -- Consulta com partition pruning
  SELECT * FROM hive_metastore.vendas_legado.vendas_part WHERE ano = 2024 AND mes = 1;
  ```
- [ ] **Ex 7.8 (Exam-style):** "Quando é recomendado particionar uma tabela Delta? Qual o tamanho mínimo recomendado por partição?"

---

## Módulo 8 — BÔNUS: Migração Hive Metastore → Unity Catalog
**Objetivo:** Entender o processo, estratégias e comandos de migração do Hive legado para Unity Catalog.

### 8.1 — Planejamento da Migração
- [ ] **Estudar os conceitos:**
  - Por que migrar: governança centralizada, lineage, auditoria, multi-workspace
  - Assessment: inventário de databases, tabelas, views e permissões existentes
  - Estratégias de migração: Big Bang vs Incremental
  - Compatibilidade: o que pode e o que não pode ser migrado diretamente
  - Impacto em jobs e notebooks existentes

#### Exercícios — Planejamento
- [ ] **Ex 8.1:** Criar checklist de pré-migração:
  - [ ] Inventário de todas as databases e tabelas no `hive_metastore`
  - [ ] Mapeamento de permissões atuais (Table ACLs)
  - [ ] Identificação de external locations usadas
  - [ ] Lista de jobs/notebooks que referenciam `hive_metastore`
  - [ ] Plano de rollback
- [ ] **Ex 8.2 (Hands-on):** Gerar inventário automatizado:
  ```python
  # Inventário de tabelas no hive_metastore
  databases = spark.sql("SHOW DATABASES IN hive_metastore").collect()
  inventario = []
  for db in databases:
      db_name = db.databaseName
      tables = spark.sql(f"SHOW TABLES IN hive_metastore.{db_name}").collect()
      for tbl in tables:
          detail = spark.sql(f"DESCRIBE DETAIL hive_metastore.{db_name}.{tbl.tableName}").collect()[0]
          inventario.append({
              "database": db_name,
              "table": tbl.tableName,
              "format": detail.format,
              "location": detail.location,
              "sizeInBytes": detail.sizeInBytes
          })
  df_inventario = spark.createDataFrame(inventario)
  df_inventario.display()
  ```

### 8.2 — Execução da Migração
- [ ] **Estudar os métodos:**
  - **UPGRADE:** `ALTER TABLE hive_metastore.db.tabela SET OWNER TO ...` (não migra)
  - **CLONE:** Copiar dados com `DEEP CLONE` / `SHALLOW CLONE`
  - **CTAS:** Recriar tabelas com `CREATE TABLE ... AS SELECT`
  - **UCX Tool:** Ferramenta oficial da Databricks para migração em massa

#### Exercícios — Execução
- [ ] **Ex 8.3 (Hands-on):** Migrar tabela com DEEP CLONE:
  ```sql
  -- Criar estrutura de destino no Unity Catalog
  CREATE CATALOG IF NOT EXISTS empresa_prod;
  CREATE SCHEMA IF NOT EXISTS empresa_prod.vendas;

  -- Migrar com DEEP CLONE (copia dados)
  CREATE TABLE empresa_prod.vendas.transacoes
  DEEP CLONE hive_metastore.vendas_legado.transacoes;

  -- Verificar migração
  SELECT count(*) FROM empresa_prod.vendas.transacoes;
  DESCRIBE DETAIL empresa_prod.vendas.transacoes;
  ```
- [ ] **Ex 8.4 (Hands-on):** Migrar tabela com CTAS (quando CLONE não é possível, ex: tabela não-Delta):
  ```sql
  CREATE TABLE empresa_prod.vendas.transacoes_parquet
  USING DELTA
  AS SELECT * FROM hive_metastore.vendas_legado.parquet_table;
  ```
- [ ] **Ex 8.5 (Hands-on):** Migrar permissões:
  ```sql
  -- Verificar permissões atuais no hive_metastore
  SHOW GRANTS ON DATABASE hive_metastore.vendas_legado;

  -- Recriar no Unity Catalog
  GRANT USAGE ON CATALOG empresa_prod TO `data_engineers`;
  GRANT USAGE ON SCHEMA empresa_prod.vendas TO `data_engineers`;
  GRANT SELECT ON SCHEMA empresa_prod.vendas TO `analysts`;
  ```

### 8.3 — Pós-migração
- [ ] **Estudar:**
  - Validação de dados (contagem, checksums, amostragem)
  - Atualização de referências em notebooks e jobs
  - Período de coexistência (dual-read)
  - Deprecação do hive_metastore

#### Exercícios — Pós-migração
- [ ] **Ex 8.6 (Hands-on):** Script de validação pós-migração:
  ```python
  def validar_migracao(tabela_origem, tabela_destino):
      count_origem = spark.sql(f"SELECT count(*) as cnt FROM {tabela_origem}").collect()[0].cnt
      count_destino = spark.sql(f"SELECT count(*) as cnt FROM {tabela_destino}").collect()[0].cnt

      assert count_origem == count_destino, f"ERRO: contagem diferente! {count_origem} vs {count_destino}"

      # Comparar schema
      schema_origem = spark.table(tabela_origem).schema
      schema_destino = spark.table(tabela_destino).schema
      assert schema_origem == schema_destino, "ERRO: schemas diferentes!"

      print(f"OK: {tabela_destino} migrada com sucesso ({count_destino} registros)")

  validar_migracao(
      "hive_metastore.vendas_legado.transacoes",
      "empresa_prod.vendas.transacoes"
  )
  ```
- [ ] **Ex 8.7:** Criar um script para atualizar referências em notebooks (substituir `hive_metastore.db.` por `catalog.schema.`)
- [ ] **Ex 8.8:** Documentar um runbook de migração com etapas, responsáveis e critérios de go/no-go
- [ ] **Ex 8.9 (Exam-style):** "Qual comando copia dados e metadados de uma tabela Delta para outra localização?"
- [ ] **Ex 8.10 (Exam-style):** "Durante a migração, como garantir que jobs existentes continuem funcionando enquanto as tabelas são migradas?"

---

## Módulo 9 — Certificação, Mindset e Preparação Final
**Objetivo:** Preparação estratégica para a prova e desenvolvimento de carreira.

### Vídeo 13 — Dica para Passar na Certificação Databricks (2:33)
- [ ] Assistir ao vídeo
- [ ] Anotar todas as dicas mencionadas
- [ ] **Ações práticas:**
  - Baixar o [Exam Guide — Data Engineer Associate](https://www.databricks.com/learn/certification/data-engineer-associate)
  - Mapear cada tópico do exam guide para o módulo correspondente deste plano
  - Identificar gaps de conhecimento

### Vídeo 14 — Databricks Free vai Revolucionar o Aprendizado (19:42)
- [ ] Assistir ao vídeo
- [ ] **Ação prática:**
  - Criar conta no Databricks Free (se ainda não tiver)
  - Completar todos os exercícios hands-on pendentes
  - Explorar os cursos gratuitos da Databricks Academy

### Vídeo 15 — Mentalidade para Crescer na Carreira em Dados (11:53)
- [ ] Assistir ao vídeo
- [ ] **Reflexão pessoal:**
  - Definir objetivos de carreira em dados para os próximos 6 meses
  - Estabelecer rotina de estudos consistente
  - Agendar a data do exame (accountability)

---

## Módulo 10 — Simulados e Revisão Final (Semana 5–6)
**Objetivo:** Consolidar conhecimento e garantir nota acima de 70%.

### Revisão por Domínio do Exame

#### Databricks Lakehouse Platform (~24%)
- [ ] Revisar: Arquitetura Lakehouse, Clusters, Notebooks, Workspace
- [ ] Refazer exercícios dos Módulos 1 e 2
- [ ] **Simulado — 10 questões:** responder sem consulta e corrigir

#### ELT with Spark SQL and Python (~29%)
- [ ] Revisar: PySpark, SparkSQL, Delta Lake, MERGE, Time Travel
- [ ] Refazer exercícios dos Módulos 3 e 4
- [ ] **Simulado — 13 questões:** responder sem consulta e corrigir

#### Incremental Data Processing (~16%)
- [ ] Revisar: Structured Streaming, Auto Loader, `cloudFiles`, Trigger modes
- [ ] **Estudar conceitos extras:**
  - `spark.readStream` vs `spark.read`
  - Trigger: `availableNow` vs `processingTime`
  - Checkpointing
  - Auto Loader: schema inference e schema evolution
- [ ] **Simulado — 7 questões:** responder sem consulta e corrigir

#### Production Pipelines (~16%)
- [ ] Revisar: Workflows, DLT, Expectations, Medallion
- [ ] Refazer exercícios dos Módulos 5
- [ ] **Simulado — 7 questões:** responder sem consulta e corrigir

#### Data Governance (~11%)
- [ ] Revisar: Unity Catalog, permissões, lineage, hierarquia
- [ ] Refazer exercícios dos Módulos 6 e 7
- [ ] **Simulado — 5 questões:** responder sem consulta e corrigir

### Recursos para Simulados
- [ ] [Databricks Academy — Practice Exam](https://www.databricks.com/learn/certification/data-engineer-associate) (oficial)
- [ ] Fazer pelo menos 3 simulados completos (45 questões cada)
- [ ] Meta: acertar consistentemente **80%+** antes de agendar o exame
- [ ] Revisar cada questão errada e mapear para o módulo correspondente

### Flashcards — Temas Obrigatórios
- [ ] Criar flashcards para:
  - Diferença Lakehouse vs Data Lake vs Data Warehouse
  - All-Purpose vs Job Cluster
  - Managed vs External Table
  - MERGE syntax
  - Time Travel (VERSION AS OF / TIMESTAMP AS OF)
  - OPTIMIZE + ZORDER
  - VACUUM + retenção
  - Schema Enforcement vs Schema Evolution
  - Temp View vs Global Temp View
  - DLT: expect vs expect_or_drop vs expect_or_fail
  - Auto Loader vs COPY INTO
  - Unity Catalog: hierarquia completa
  - GRANT USAGE vs GRANT SELECT
  - Streaming Table vs Materialized View
  - Trigger modes (availableNow, processingTime)
  - DEEP CLONE vs SHALLOW CLONE

---

## Checklist Pré-Exame

- [ ] Todos os 10 módulos concluídos
- [ ] Todos os exercícios hands-on realizados no Databricks Free
- [ ] Módulo bônus de migração Hive → UC finalizado
- [ ] Exam Guide oficial revisado — todos os tópicos mapeados
- [ ] Mínimo 3 simulados completos realizados
- [ ] Nota consistente acima de 80% nos simulados
- [ ] Flashcards revisados nos últimos 3 dias
- [ ] Questões erradas re-estudadas
- [ ] Exame agendado
- [ ] Ambiente de prova testado (webcam, internet, documento de identidade)

---

## Cronograma Sugerido

| Dia | Atividade | Tempo |
|-----|-----------|-------|
| Dia 1 | Módulo 1 (Vídeos 1–3) + exercícios | 2h |
| Dia 2 | Módulo 2 (Vídeos 4–6) + exercícios hands-on | 2h30 |
| Dia 3 | Módulo 3 — Vídeo 7 + exercícios PySpark/SQL | 3h |
| Dia 4 | Módulo 3 — Vídeo 9 + pipeline ELT completo | 2h30 |
| Dia 5 | Módulo 4 — Delta Lake profundo + exercícios | 3h |
| Dia 6 | Módulo 5 — Vídeo 10 (Workflows) + exercícios | 2h |
| Dia 7 | Módulo 5 — Vídeo 11 (DLT) + exercícios | 2h |
| Dia 8 | Módulo 6 — Vídeo 12 + Unity Catalog aprofundado | 2h30 |
| Dia 9 | Módulo 7 — Hive Metastore + exercícios | 2h30 |
| Dia 10 | Módulo 8 — Migração Hive → UC (parte 1) | 2h30 |
| Dia 11 | Módulo 8 — Migração Hive → UC (parte 2) + validação | 2h |
| Dia 12 | Módulo 9 — Vídeos 13–15 + mapeamento do Exam Guide | 2h |
| Dia 13 | Módulo 10 — Incremental Data Processing (estudo extra) | 2h |
| Dia 14 | Módulo 10 — Simulado 1 completo + correção | 2h30 |
| Dia 15 | Revisão dos erros do simulado 1 + flashcards | 2h |
| Dia 16 | Simulado 2 completo + correção | 2h30 |
| Dia 17 | Simulado 3 completo + revisão final | 2h30 |
| Dia 18 | **DIA DO EXAME** | 1h30 |

**Tempo total estimado:** ~40–45 horas

---

> **Dica final:** O exame Data Engineer Associate é prático e baseado em cenários reais. Não basta decorar — você precisa saber **quando** e **por que** usar cada recurso. Reproduza todos os exercícios hands-on no Databricks Free. A prática é o que separa quem passa de quem não passa.
