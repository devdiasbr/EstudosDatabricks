# Simulado Modelo — Data Engineer Associate

20 questões organizadas por domínio. Tempo sugerido: 40 minutos.
Responda sem consultar anotações. Meta: 80%+

---

## Lakehouse Platform (5 questões)

### Q1
Qual das alternativas melhor descreve a arquitetura Lakehouse?

- a) Um Data Warehouse na nuvem com suporte a machine learning
- b) Uma combinação do armazenamento barato do Data Lake com transações ACID e performance do Data Warehouse
- c) Um Data Lake com camada de visualização
- d) Um banco de dados relacional distribuído

### Q2
Qual tipo de cluster é criado automaticamente quando um Job é executado e destruído após a conclusão?

- a) All-Purpose Cluster
- b) Interactive Cluster
- c) Job Cluster
- d) Shared Cluster

### Q3
Qual magic command permite executar um notebook dentro de outro, compartilhando variáveis?

- a) `%include`
- b) `%exec`
- c) `%run`
- d) `%import`

### Q4
Um Data Engineer precisa listar os arquivos em um diretório no DBFS. Qual comando deve usar?

- a) `%fs ls /path/`
- b) `%list /path/`
- c) `%dir /path/`
- d) `%show /path/`

### Q5
Qual configuração de cluster permite que o número de workers aumente ou diminua automaticamente com base na carga?

- a) Auto-termination
- b) Autoscaling
- c) Spot instances
- d) Cluster policies

---

## ELT with Spark SQL and Python (6 questões)

### Q6
Qual opção é necessária ao ler um CSV para que a primeira linha seja usada como nomes de coluna?

- a) `.option("inferSchema", "true")`
- b) `.option("header", "true")`
- c) `.option("firstRow", "header")`
- d) `.option("columns", "auto")`

### Q7
Qual tipo de JOIN retorna apenas os registros da tabela esquerda que NÃO têm correspondência na tabela direita?

- a) INNER JOIN
- b) LEFT JOIN
- c) ANTI JOIN
- d) FULL OUTER JOIN

### Q8
Após executar várias operações em uma tabela Delta, qual comando compacta small files?

- a) COMPACT
- b) OPTIMIZE
- c) VACUUM
- d) DEFRAGMENT

### Q9
Qual é a retenção padrão do comando VACUUM?

- a) 24 horas
- b) 72 horas
- c) 168 horas (7 dias)
- d) 720 horas (30 dias)

### Q10
Qual opção permite adicionar novas colunas automaticamente ao escrever dados em uma tabela Delta existente?

- a) `.option("autoSchema", "true")`
- b) `.option("mergeSchema", "true")`
- c) `.option("evolveSchema", "true")`
- d) `.option("addColumns", "true")`

### Q11
O que acontece com os registros existentes quando uma nova coluna é adicionada via mergeSchema?

- a) São deletados e recriados
- b) Recebem o valor padrão definido pelo schema
- c) Recebem NULL na nova coluna
- d) O append falha

---

## Incremental Data Processing (3 questões)

### Q12
Qual formato é usado pelo Auto Loader para detectar novos arquivos automaticamente?

- a) `spark.readStream.format("delta")`
- b) `spark.readStream.format("cloudFiles")`
- c) `spark.readStream.format("autoloader")`
- d) `spark.readStream.format("stream")`

### Q13
Qual trigger mode processa todos os dados disponíveis e depois para o stream?

- a) `trigger(processingTime="1 minute")`
- b) `trigger(continuous=True)`
- c) `trigger(availableNow=True)`
- d) `trigger(once=False)`

### Q14
Qual é o propósito do checkpoint location em Structured Streaming?

- a) Armazenar dados em cache para melhor performance
- b) Rastrear o progresso do stream para retomar após falhas
- c) Definir o schema dos dados de entrada
- d) Limitar o volume de dados processados por batch

---

## Production Pipelines (3 questões)

### Q15
Como passar dados entre tasks em um Workflow do Databricks?

- a) Usando variáveis globais Python
- b) Usando `dbutils.jobs.taskValues.set()` e `.get()`
- c) Usando `spark.conf.set()`
- d) Usando widgets compartilhados

### Q16
Em Delta Live Tables, qual decorator remove registros que violam uma regra de qualidade?

- a) `@dlt.expect`
- b) `@dlt.expect_or_drop`
- c) `@dlt.expect_or_fail`
- d) `@dlt.validate`

### Q17
Qual camada da arquitetura Medallion contém dados limpos e validados, prontos para análise?

- a) Bronze
- b) Silver
- c) Gold
- d) Platinum

---

## Data Governance (3 questões)

### Q18
Um analista precisa ler a tabela `prod.gold.revenue`. Quais permissões mínimas são necessárias?

- a) SELECT na tabela `revenue`
- b) USAGE no catalog `prod` + SELECT na tabela `revenue`
- c) USAGE no catalog `prod` + USAGE no schema `gold` + SELECT na tabela `revenue`
- d) ALL PRIVILEGES no catalog `prod`

### Q19
Qual a principal diferença de escopo entre Hive Metastore e Unity Catalog?

- a) Hive é por conta, UC é por workspace
- b) Hive é por workspace, UC é por conta (multi-workspace)
- c) Ambos são por workspace
- d) Ambos são por conta

### Q20
Qual comando migra uma tabela Delta do hive_metastore para Unity Catalog, criando uma cópia independente?

- a) `COPY TABLE hive_metastore.db.t TO uc.schema.t`
- b) `CREATE TABLE uc.schema.t DEEP CLONE hive_metastore.db.t`
- c) `MIGRATE TABLE hive_metastore.db.t TO uc.schema.t`
- d) `ALTER TABLE hive_metastore.db.t SET CATALOG uc`

---

## Gabarito

Preencha suas respostas e depois confira:

| # | Sua Resposta | Gabarito | ✓/✗ |
|---|-------------|----------|-----|
| Q1 | ___ | b | |
| Q2 | ___ | c | |
| Q3 | ___ | c | |
| Q4 | ___ | a | |
| Q5 | ___ | b | |
| Q6 | ___ | b | |
| Q7 | ___ | c | |
| Q8 | ___ | b | |
| Q9 | ___ | c | |
| Q10 | ___ | b | |
| Q11 | ___ | c | |
| Q12 | ___ | b | |
| Q13 | ___ | c | |
| Q14 | ___ | b | |
| Q15 | ___ | b | |
| Q16 | ___ | b | |
| Q17 | ___ | b | |
| Q18 | ___ | c | |
| Q19 | ___ | b | |
| Q20 | ___ | b | |

**Sua nota: ___/20 (____%)**

> Meta: 16/20 (80%) ou mais. Se não atingiu, revise os domínios com mais erros.
