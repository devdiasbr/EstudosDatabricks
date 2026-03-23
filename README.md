# Databricks Certified Data Engineer Associate — Plano de Estudos

## Playlist de Apoio
**[Aprender Databricks - Curso Completo Gratuito](https://www.youtube.com/playlist?list=PLnxUgOpvXnWzTkha0174lXfES019PQz9U)**

---

## Sobre o Exame

| Item | Detalhe |
|------|---------|
| Certificação | Databricks Certified Data Engineer Associate |
| Formato | 45 questões de múltipla escolha |
| Duração | 90 minutos |
| Nota mínima | 70% |
| Custo | USD 200 |

### Domínios do Exame

| Domínio | Peso | Módulos |
|---------|------|---------|
| Databricks Lakehouse Platform | ~24% | 1, 2 |
| ELT with Spark SQL and Python | ~29% | 3, 4 |
| Incremental Data Processing | ~16% | 5, 5-B |
| Production Pipelines | ~16% | 5, 5-B |
| Data Governance | ~11% | 6, 7, 8 |

---

## Quick Start — Primeiros Passos

```
1. Importar todos os arquivos .py no Workspace do Databricks
2. Executar:  utils/setup_databases.py       → Cria databases (bronze, silver, gold)
3. Executar:  utils/gerar_dados_faker.py     → Popula tabelas com dados realistas (Faker)
4. Começar:   modulo-01-fundamentos/         → Seguir os módulos na ordem
```

### Base de Dados Gerada (Faker — locale pt_BR)

| Tabela | Registros | Descrição | Usado nos módulos |
|--------|-----------|-----------|-------------------|
| `certificacao_bronze.clientes` | 5.000 | Nome, CPF, email, cidade, estado, plano, idade | 3, 4, 6, 7, 8 |
| `certificacao_bronze.produtos` | 500 | 5 categorias, 10 marcas, preço, custo, estoque, avaliação | 3, 4, 5 |
| `certificacao_bronze.vendas` | 50.000 | Vendas com canal, pagamento, status, desconto | 3, 4, 5, 7, 8 |
| `certificacao_bronze.eventos` | 20.000 | Logs de atividade com payload JSON (login, compra, search) | 5, 6 |
| `certificacao_bronze.funcionarios` | 1.000 | Dados sensíveis (CPF, salário) para exercícios de governança | 6 |
| `certificacao_bronze.vendas_raw` | ~5.500 | Dados **sujos**: nulos, duplicatas, tipos errados, inconsistências | 3 (limpeza ELT) |
| CSVs incrementais (3 lotes) | 450 | Arquivos CSV para exercícios de Auto Loader | 5 |

> Os dados são gerados com `Faker("pt_BR")` — nomes, CPFs, cidades e endereços brasileiros.
> Seed fixa (`42`) garante reprodutibilidade.

---

## Estrutura de Pastas

```
plano-de-estudos/
│
├── README.md                          ← Você está aqui
├── plano-de-estudos-databricks.md     ← Plano detalhado completo
│
├── modulo-01-fundamentos/
│   ├── README.md                      ← Guia do módulo + links dos vídeos
│   ├── ex_01_lakehouse_comparativo.py ← Exercício: comparativo de arquiteturas
│   └── ex_02_workspace_mapa.md        ← Exercício: mapa mental do workspace
│
├── modulo-02-ambiente-pratico/
│   ├── README.md
│   ├── ex_01_setup_ambiente.py        ← Notebook: setup do ambiente
│   ├── ex_02_clusters.md              ← Exercício: comparativo de clusters
│   └── ex_03_notebook_multilingue.py  ← Notebook: magic commands e widgets
│
├── modulo-03-elt-pyspark-sql/
│   ├── README.md
│   ├── ex_01_leitura_dados.py         ← Notebook: leitura CSV/JSON/Parquet/Delta
│   ├── ex_02_transformacoes.py        ← Notebook: select, filter, groupBy, join
│   ├── ex_03_pyspark_vs_sql.py        ← Notebook: comparativo PySpark vs SQL
│   ├── ex_04_pipeline_elt.py          ← Notebook: pipeline ELT Bronze→Silver→Gold
│   └── ex_05_funcoes_avancadas.py     ← Notebook: higher-order functions, pivot
│
├── modulo-04-delta-lake/
│   ├── README.md
│   ├── ex_01_crud_delta.py            ← Notebook: INSERT, UPDATE, DELETE, MERGE
│   ├── ex_02_time_travel.py           ← Notebook: versionamento e restore
│   ├── ex_03_optimize_vacuum.py       ← Notebook: OPTIMIZE, ZORDER, VACUUM
│   ├── ex_04_schema_evolution.py      ← Notebook: enforcement vs evolution
│   └── ex_05_clone_ctas.py            ← Notebook: DEEP/SHALLOW CLONE, CTAS
│
├── modulo-05-pipelines-producao/
│   ├── README.md
│   ├── ex_01_workflow_tasks.py        ← Notebook: tasks + task values
│   ├── ex_02_dlt_medallion.py         ← Notebook: pipeline DLT declarativo
│   └── ex_03_autoloader.py            ← Notebook: Auto Loader + Structured Streaming
│
├── modulo-05b-conexoes-externas/      ← NOVO: Landing Zones & Fontes Externas
│   ├── README.md
│   ├── ex_01_cloud_storage.py        ← Notebook: Azure ADLS, AWS S3, GCP GCS
│   ├── ex_02_bancos_relacionais.py   ← Notebook: JDBC — SQL Server, PostgreSQL, MySQL, Oracle
│   ├── ex_03_bancos_nosql.py         ← Notebook: MongoDB, Cosmos DB, DynamoDB
│   ├── ex_04_apis_arquivos.py        ← Notebook: REST APIs + formatos (CSV, JSON, Parquet, Avro, XML)
│   ├── ex_05_secrets_seguranca.py    ← Notebook: Databricks Secrets, Key Vault, IAM
│   └── ex_06_landing_to_bronze.py    ← Notebook: pipeline completo landing → bronze
│
├── modulo-06-unity-catalog/
│   ├── README.md
│   ├── ex_01_hierarquia_uc.py         ← Notebook: catalog → schema → table
│   ├── ex_02_permissoes.py            ← Notebook: GRANT, REVOKE, SHOW GRANTS
│   └── ex_03_dynamic_views.py         ← Notebook: views com mascaramento
│
├── modulo-07-hive-metastore/
│   ├── README.md
│   ├── ex_01_explorar_hive.py         ← Notebook: navegar hive_metastore
│   ├── ex_02_formatos_tabelas.py      ← Notebook: Parquet vs Delta vs CSV
│   └── ex_03_particionamento.py       ← Notebook: partições e pruning
│
├── modulo-08-migracao-hive-uc/        ← BÔNUS
│   ├── README.md
│   ├── ex_01_inventario.py            ← Notebook: inventário automatizado
│   ├── ex_02_migracao_clone.py        ← Notebook: migração com DEEP CLONE
│   ├── ex_03_migracao_ctas.py         ← Notebook: migração com CTAS
│   ├── ex_04_migrar_permissoes.py     ← Notebook: recriar permissões no UC
│   └── ex_05_validacao.py             ← Notebook: validação pós-migração
│
├── modulo-09-certificacao/
│   ├── README.md
│   └── exam_guide_checklist.md        ← Checklist do Exam Guide (48 tópicos)
│
├── modulo-10-simulados-revisao/
│   ├── README.md
│   ├── flashcards.md                  ← 25+ flashcards dos conceitos-chave
│   └── simulado_modelo.md             ← 20 questões com gabarito
│
└── utils/
    ├── setup_databases.py             ← 1o: criar estrutura de databases
    ├── gerar_dados_faker.py           ← 2o: popular tabelas com Faker (pt_BR)
    └── helpers.py                     ← Funções auxiliares reutilizáveis
```

---

## Como Usar

### 1. Setup inicial (execute uma vez)

```
utils/setup_databases.py     → Cria databases certificacao_bronze, silver, gold
utils/gerar_dados_faker.py   → Instala Faker e gera ~82.000 registros
```

### 2. Estudar os módulos (na ordem)

1. **Assista aos vídeos** linkados no `README.md` de cada módulo
2. **Execute os notebooks `.py`** no Databricks — todos usam os dados do Faker
3. **Complete os exercícios** marcados com `TODO: Seu código aqui`
4. **Responda as perguntas Exam-Style** sem consultar anotações
5. **Marque os checkboxes** conforme avança

### 3. Revisão final

1. Complete o `exam_guide_checklist.md` (Módulo 9)
2. Estude os `flashcards.md` (Módulo 10)
3. Faça o `simulado_modelo.md` — meta: 80%+
4. Faça simulados oficiais da Databricks Academy

### Importando notebooks no Databricks

Os arquivos `.py` usam o formato de notebook do Databricks:
1. No Workspace, clique em **Import**
2. Selecione o arquivo `.py`
3. O Databricks reconhece automaticamente o formato e cria o notebook

> **Dica:** Importe a pasta inteira para manter a estrutura organizada.

---

## Cronograma Sugerido (18 dias)

| Dia | Atividade | Tempo |
|-----|-----------|-------|
| 0 | Setup: `setup_databases.py` + `gerar_dados_faker.py` | 30min |
| 1 | Módulo 1 — Fundamentos do Lakehouse | 2h |
| 2 | Módulo 2 — Clusters, Notebooks, Workspace | 2h30 |
| 3–4 | Módulo 3 — ELT com PySpark e SparkSQL | 3h + 2h30 |
| 5 | Módulo 4 — Delta Lake Profundo | 3h |
| 6–7 | Módulo 5 — Workflows, DLT, Auto Loader | 2h + 2h |
| 7–8 | Módulo 5-B — Conexões Externas (Cloud, JDBC, NoSQL, APIs) | 3h + 2h |
| 9 | Módulo 6 — Unity Catalog & Governança | 2h30 |
| 9 | Módulo 7 — Hive Metastore (legado) | 2h30 |
| 10–11 | Módulo 8 — Migração Hive → UC (bônus) | 2h30 + 2h |
| 12 | Módulo 9 — Exam Guide + Preparação | 2h |
| 13–17 | Módulo 10 — Simulados e Revisão | 2h/dia |
| 18 | **DIA DO EXAME** | 1h30 |

**Tempo total: ~40–45 horas**

---

## Números do Plano

| Métrica | Quantidade |
|---------|-----------|
| Módulos | 11 |
| Notebooks (.py) | 36 |
| Exercícios hands-on | 100+ |
| Perguntas exam-style | 55+ |
| Flashcards | 25+ |
| Questões de simulado | 20 (modelo) |
| Registros gerados (Faker) | ~82.000 |
| Tópicos do Exam Guide mapeados | 48 |

---

> **Dica final:** O exame Data Engineer Associate é prático e baseado em cenários reais. Não basta decorar — reproduza TODOS os exercícios no Databricks Free. A prática com dados reais (Faker) é o que separa quem passa de quem não passa.
