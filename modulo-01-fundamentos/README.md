# Módulo 1 — Fundamentos do Databricks & Lakehouse Platform

**Domínio do exame:** Databricks Lakehouse Platform (~24%)
**Tempo estimado:** 2 horas

---

## Vídeos de Apoio

| # | Título | Duração | Link |
|---|--------|---------|------|
| 1 | Curso Aprendendo Databricks - Boas Vindas | 3:24 | [Assistir](https://www.youtube.com/watch?v=YOUR_VIDEO_1&list=PLnxUgOpvXnWzTkha0174lXfES019PQz9U&index=1) |
| 2 | O que é Databricks? | 8:28 | [Assistir](https://www.youtube.com/watch?v=YOUR_VIDEO_2&list=PLnxUgOpvXnWzTkha0174lXfES019PQz9U&index=2) |
| 3 | Workspace | 7:41 | [Assistir](https://www.youtube.com/watch?v=YOUR_VIDEO_3&list=PLnxUgOpvXnWzTkha0174lXfES019PQz9U&index=3) |

> **Dica:** Abra a [playlist completa](https://www.youtube.com/playlist?list=PLnxUgOpvXnWzTkha0174lXfES019PQz9U) e assista os vídeos 1 a 3.

---

## Objetivos de Aprendizado

Ao final deste módulo você deve saber:
- [ ] Explicar o que é Databricks e a arquitetura Lakehouse
- [ ] Diferenciar Data Lake, Data Warehouse e Lakehouse
- [ ] Descrever os componentes do Workspace
- [ ] Entender o papel do Apache Spark no Databricks

---

## Conceitos-chave

### Lakehouse Platform
- Combina o melhor do Data Lake (armazenamento barato, dados brutos) com Data Warehouse (performance, ACID, schema)
- Formato aberto (Delta Lake) sobre armazenamento em nuvem (S3, ADLS, GCS)
- Uma única plataforma para DE, DS, ML e Analytics

### Workspace
- Interface web do Databricks
- Componentes: Notebooks, Repos, Clusters, Jobs, DBSQL, Unity Catalog
- Organização por pastas e permissões

---

## Exercícios

| Arquivo | Descrição | Tipo |
|---------|-----------|------|
| `ex_01_lakehouse_comparativo.py` | Comparativo entre arquiteturas de dados | Notebook Databricks |
| `ex_02_workspace_mapa.md` | Mapa mental do Workspace | Markdown |

---

## Perguntas Exam-Style

1. "Uma empresa precisa de analytics em tempo real com dados estruturados e não-estruturados, além de suporte a ML. Qual arquitetura é mais adequada e por quê?"
2. "Qual a principal vantagem do Lakehouse em relação a um Data Lake tradicional?"
3. "Qual componente do Databricks permite organizar notebooks, repos e dados em uma interface centralizada?"
