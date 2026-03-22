# Módulo 5 — Pipelines de Produção (Workflows + DLT + Auto Loader)

**Domínio do exame:** Production Pipelines (~16%) + Incremental Data Processing (~16%)
**Tempo estimado:** 4 horas (2 dias)

---

## Vídeos de Apoio

| # | Título | Duração | Link |
|---|--------|---------|------|
| 10 | Automação de Pipelines com Workflows | 9:17 | [Assistir](https://www.youtube.com/watch?v=YOUR_VIDEO_10&list=PLnxUgOpvXnWzTkha0174lXfES019PQz9U&index=10) |
| 11 | Delta Live Tables | 4:40 | [Assistir](https://www.youtube.com/watch?v=YOUR_VIDEO_11&list=PLnxUgOpvXnWzTkha0174lXfES019PQz9U&index=11) |

> **Dica:** Abra a [playlist completa](https://www.youtube.com/playlist?list=PLnxUgOpvXnWzTkha0174lXfES019PQz9U) e assista os vídeos 10 e 11.

---

## Objetivos de Aprendizado

- [ ] Criar e configurar Workflows com múltiplas tasks
- [ ] Entender scheduling, dependências e alertas
- [ ] Usar task values para comunicação entre tasks
- [ ] Criar pipelines declarativos com Delta Live Tables
- [ ] Aplicar Expectations (qualidade de dados)
- [ ] Entender Auto Loader e Structured Streaming
- [ ] Diferenciar Streaming Table vs Materialized View

---

## Exercícios

| Arquivo | Descrição | Tipo |
|---------|-----------|------|
| `ex_01_workflow_tasks.py` | Tasks, task values, DAG de pipeline | Notebook |
| `ex_02_dlt_medallion.py` | Pipeline DLT declarativo (Bronze→Silver→Gold) | Notebook |
| `ex_03_autoloader.py` | Auto Loader + Structured Streaming | Notebook |
