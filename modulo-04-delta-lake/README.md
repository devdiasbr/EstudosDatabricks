# Módulo 4 — Delta Lake Profundo

**Domínio do exame:** ELT with Spark SQL and Python (~29%) + Incremental Data Processing
**Tempo estimado:** 3 horas

---

## Vídeos de Apoio

| # | Título | Duração | Link |
|---|--------|---------|------|
| 8 | Delta Lake e Databases do Databricks | 11:42 | [Assistir](https://www.youtube.com/watch?v=YOUR_VIDEO_8&list=PLnxUgOpvXnWzTkha0174lXfES019PQz9U&index=8) |

> **Dica:** Abra a [playlist completa](https://www.youtube.com/playlist?list=PLnxUgOpvXnWzTkha0174lXfES019PQz9U) e assista o vídeo 8. Este é um dos tópicos mais cobrados no exame.

---

## Objetivos de Aprendizado

- [ ] Entender a arquitetura Delta (Parquet + Transaction Log)
- [ ] Executar operações DML: INSERT, UPDATE, DELETE, MERGE
- [ ] Usar Time Travel para consultar versões anteriores
- [ ] Aplicar OPTIMIZE com Z-Ordering
- [ ] Entender VACUUM e retenção de dados
- [ ] Diferenciar Schema Enforcement vs Schema Evolution
- [ ] Usar DEEP CLONE e SHALLOW CLONE

---

## Exercícios

| Arquivo | Descrição | Tipo |
|---------|-----------|------|
| `ex_01_crud_delta.py` | INSERT, UPDATE, DELETE + MERGE (upsert) | Notebook |
| `ex_02_time_travel.py` | Versionamento, DESCRIBE HISTORY, RESTORE | Notebook |
| `ex_03_optimize_vacuum.py` | OPTIMIZE, ZORDER, VACUUM | Notebook |
| `ex_04_schema_evolution.py` | Schema Enforcement vs Evolution | Notebook |
| `ex_05_clone_ctas.py` | DEEP CLONE, SHALLOW CLONE, CTAS | Notebook |
