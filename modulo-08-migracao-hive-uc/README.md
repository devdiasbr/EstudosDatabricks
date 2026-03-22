# Módulo 8 — BÔNUS: Migração Hive Metastore → Unity Catalog

**Tipo:** Módulo bônus (diferencial profissional)
**Tempo estimado:** 4h30 (2 dias)

---

## Vídeos de Apoio

> Módulo complementar à playlist. Baseia-se nos conceitos dos módulos 6 (Unity Catalog) e 7 (Hive Metastore).
> [Playlist completa](https://www.youtube.com/playlist?list=PLnxUgOpvXnWzTkha0174lXfES019PQz9U)

---

## Objetivos de Aprendizado

- [ ] Planejar uma migração Hive → Unity Catalog
- [ ] Gerar inventário automatizado de tabelas e permissões
- [ ] Executar migração com DEEP CLONE e CTAS
- [ ] Migrar permissões de Table ACLs para GRANT/REVOKE
- [ ] Validar dados pós-migração (contagem, schema, amostragem)
- [ ] Entender estratégias de coexistência e deprecação

---

## Exercícios

| Arquivo | Descrição | Tipo |
|---------|-----------|------|
| `ex_01_inventario.py` | Inventário automatizado do hive_metastore | Notebook |
| `ex_02_migracao_clone.py` | Migração com DEEP CLONE | Notebook |
| `ex_03_migracao_ctas.py` | Migração com CTAS (tabelas não-Delta) | Notebook |
| `ex_04_migrar_permissoes.py` | Recriar permissões no Unity Catalog | Notebook |
| `ex_05_validacao.py` | Validação pós-migração | Notebook |
