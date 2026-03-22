# Exercício 2 — Clusters no Databricks

**Módulo 2 — Ambiente Prático**

**Vídeo de apoio:** [Clusters](https://www.youtube.com/playlist?list=PLnxUgOpvXnWzTkha0174lXfES019PQz9U) — Vídeo 5

---

## Exercício 2.1 — Tabela Comparativa

Preencha a tabela comparativa entre os tipos de cluster:

| Característica | All-Purpose Cluster | Job Cluster |
|---------------|-------------------|-------------|
| Criado por | ___ | ___ |
| Uso principal | ___ | ___ |
| Custo | ___ | ___ |
| Compartilhável | ___ | ___ |
| Ciclo de vida | ___ | ___ |
| Quando usar | ___ | ___ |

---

## Exercício 2.2 — Configurações de Cluster

Explique cada configuração:

| Configuração | O que faz | Valor recomendado para estudos |
|-------------|-----------|-------------------------------|
| Databricks Runtime | ___ | ___ |
| Worker Type | ___ | ___ |
| Min/Max Workers | ___ | ___ |
| Autoscaling | ___ | ___ |
| Auto-termination | ___ | ___ |
| Spot Instances | ___ | ___ |

---

## Exercício 2.3 — Perguntas Exam-Style

### Pergunta 1
Um pipeline de produção roda diariamente às 02:00. Que tipo de cluster deve ser usado e por quê?

> _Sua resposta..._

### Pergunta 2
Qual configuração de cluster reduz custos quando a carga de trabalho é variável?

> _Sua resposta..._

### Pergunta 3
O que acontece com os dados em memória quando um cluster é terminado vs reiniciado?

> _Sua resposta..._

### Pergunta 4
Um Data Engineer precisa testar código interativamente e compartilhar o cluster com um colega. Qual tipo de cluster ele deve usar?

> _Sua resposta..._

### Pergunta 5
Cluster policies são usadas para quê? Quem pode criá-las?

> _Sua resposta..._

---

## Exercício 2.4 — Hands-on

- [ ] Criar um cluster All-Purpose com:
  - Runtime: mais recente LTS
  - Autoscaling: 1–2 workers
  - Auto-termination: 30 minutos
- [ ] Observar o tempo de inicialização
- [ ] Verificar a aba "Spark UI" depois de rodar uma query
- [ ] Terminar o cluster manualmente
