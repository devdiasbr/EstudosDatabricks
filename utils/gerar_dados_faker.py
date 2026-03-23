# Databricks notebook source

# MAGIC %md
# MAGIC # Gerador de Dados — Faker (pt_BR)
# MAGIC
# MAGIC **Execute este notebook UMA VEZ antes de começar os exercícios.**
# MAGIC
# MAGIC Gera dados realistas em português brasileiro para todos os módulos do curso.
# MAGIC
# MAGIC | Tabela | Registros | Usado nos módulos |
# MAGIC |--------|-----------|-------------------|
# MAGIC | `certificacao_bronze.clientes` | 5.000 | 3, 4, 6, 7, 8 |
# MAGIC | `certificacao_bronze.produtos` | 500 | 3, 4, 5, 7 |
# MAGIC | `certificacao_bronze.vendas` | 50.000 | 3, 4, 5, 7, 8 |
# MAGIC | `certificacao_bronze.eventos` | 20.000 | 5, 6 |
# MAGIC | `certificacao_bronze.funcionarios` | 1.000 | 6 (governança) |
# MAGIC | `certificacao_bronze.vendas_raw` | ~5.500 | 3 (limpeza ELT) |
# MAGIC | CSVs incrementais — 3 lotes | ~150/lote | 5 (Auto Loader) |
# MAGIC
# MAGIC > **Seed `42`** — resultados sempre idênticos. Pode re-executar sem problemas.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Passo 1 — Instalar Faker

# COMMAND ----------

# MAGIC %pip install faker==24.0.0 --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Passo 2 — Imports e configuração

# COMMAND ----------

import random
import json
from datetime import date, datetime
from faker import Faker
from pyspark.sql import Row

fake = Faker("pt_BR")
Faker.seed(42)
random.seed(42)

print("Faker carregado — locale: pt_BR | seed: 42")
print(f"Exemplo → nome: {fake.name()} | cidade: {fake.city()} | CPF: {fake.cpf()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Passo 3 — Criar databases

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS certificacao_bronze COMMENT 'Dados brutos — Faker pt_BR';
# MAGIC CREATE DATABASE IF NOT EXISTS certificacao_silver COMMENT 'Dados limpos e validados';
# MAGIC CREATE DATABASE IF NOT EXISTS certificacao_gold   COMMENT 'Dados agregados para consumo';
# MAGIC SHOW DATABASES LIKE 'certificacao*';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Passo 4 — CLIENTES (5.000 registros)

# COMMAND ----------

estados_capitais = {
    "SP": ["São Paulo", "Campinas", "Santos", "Ribeirão Preto", "Sorocaba"],
    "RJ": ["Rio de Janeiro", "Niterói", "Petrópolis", "Volta Redonda"],
    "MG": ["Belo Horizonte", "Uberlândia", "Juiz de Fora", "Contagem"],
    "RS": ["Porto Alegre", "Caxias do Sul", "Pelotas", "Santa Maria"],
    "PR": ["Curitiba", "Londrina", "Maringá", "Foz do Iguaçu"],
    "BA": ["Salvador", "Feira de Santana", "Vitória da Conquista"],
    "GO": ["Goiânia", "Anápolis", "Aparecida de Goiânia"],
    "PE": ["Recife", "Caruaru", "Petrolina"],
    "CE": ["Fortaleza", "Caucaia", "Juazeiro do Norte"],
    "SC": ["Florianópolis", "Joinville", "Blumenau"],
}
planos       = ["basico", "standard", "premium", "enterprise"]
plano_pesos  = [0.40, 0.30, 0.20, 0.10]

clientes = []
for i in range(1, 5001):
    estado = random.choice(list(estados_capitais.keys()))
    cidade = random.choice(estados_capitais[estado])
    plano  = random.choices(planos, weights=plano_pesos)[0]
    dt_cad = fake.date_between(start_date=date(2020, 1, 1), end_date=date(2024, 12, 31))
    clientes.append(Row(
        cliente_id    = i,
        nome          = fake.name(),
        email         = fake.email(),
        cpf           = fake.cpf(),
        telefone      = fake.phone_number(),
        cidade        = cidade,
        estado        = estado,
        cep           = fake.postcode(),
        plano         = plano,
        idade         = random.randint(18, 75),
        data_cadastro = dt_cad,
        ativo         = random.random() > 0.08,   # 92% ativos
    ))

df_clientes = spark.createDataFrame(clientes)
df_clientes.write.format("delta").mode("overwrite").saveAsTable("certificacao_bronze.clientes")
print(f"✓ clientes: {df_clientes.count():,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Passo 5 — PRODUTOS (500 registros)

# COMMAND ----------

categorias_itens = {
    "eletronicos":   ["Notebook", "Tablet", "Smartphone", "Monitor", "Smart TV",
                      "Headset", "Webcam", "Impressora", "Projetor", "Roteador"],
    "perifericos":   ["Mouse", "Teclado", "Hub USB", "Mousepad Gamer", "Suporte Notebook",
                      "Carregador Portátil", "Cabo HDMI", "Adaptador USB-C", "Cooler", "Leitor de Cartão"],
    "armazenamento": ["SSD 256GB", "SSD 512GB", "SSD 1TB", "HD Externo 1TB",
                      "HD Externo 2TB", "Pen Drive 64GB", "Pen Drive 128GB",
                      "Cartão SD 64GB", "NAS", "Backup Drive"],
    "software":      ["Antivírus", "Pacote Office", "Adobe Creative Cloud", "VPN Anual",
                      "Cloud Storage 1TB", "Password Manager", "Project Manager",
                      "Design Tool", "IDE Premium", "Backup Cloud"],
    "acessorios":    ["Case Notebook", "Mochila Gamer", "Suporte Monitor",
                      "Filtro de Linha", "Mesa Digitalizadora", "Caixa de Som",
                      "Base Refrigeradora", "Luminária LED", "Organizador Cabos", "Ergonomic Kit"],
}
marcas = ["Dell", "Lenovo", "HP", "Samsung", "LG",
          "Logitech", "Razer", "Corsair", "Apple", "Multilaser"]
faixa_preco = {
    "eletronicos":   (500,  12000),
    "perifericos":   (30,     800),
    "armazenamento": (50,    1500),
    "software":      (20,     600),
    "acessorios":    (15,     400),
}

produtos_rows = []
pid = 1
for cat, itens in categorias_itens.items():
    for item in itens:
        for marca in random.sample(marcas, k=random.randint(1, 3)):
            if pid > 500:
                break
            pmin, pmax = faixa_preco[cat]
            preco  = round(random.uniform(pmin, pmax), 2)
            custo  = round(preco * random.uniform(0.35, 0.65), 2)
            margem = round((preco - custo) / preco * 100, 1)
            produtos_rows.append(Row(
                produto_id    = pid,
                nome          = f"{marca} {item}",
                categoria     = cat,
                marca         = marca,
                preco         = preco,
                custo         = custo,
                margem_percent= margem,
                estoque       = random.randint(0, 500),
                avaliacao     = round(random.uniform(3.0, 5.0), 1),
                ativo         = random.random() > 0.05,
            ))
            pid += 1

df_produtos = spark.createDataFrame(produtos_rows)
df_produtos.write.format("delta").mode("overwrite").saveAsTable("certificacao_bronze.produtos")
print(f"✓ produtos: {df_produtos.count():,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Passo 6 — VENDAS (50.000 registros, particionado por ano/mes)

# COMMAND ----------

canais        = ["site", "app", "loja_fisica", "televendas", "marketplace"]
canal_pesos   = [0.35, 0.30, 0.15, 0.10, 0.10]
pagamentos    = ["cartao_credito", "cartao_debito", "pix", "boleto", "vale"]
pag_pesos     = [0.35, 0.20, 0.30, 0.10, 0.05]
status_lista  = ["concluida", "concluida", "concluida", "cancelada", "devolvida"]

# Dicionários para lookups rápidos
cliente_ids   = [r.cliente_id  for r in df_clientes.select("cliente_id").collect()]
produto_ids   = [r.produto_id  for r in df_produtos.select("produto_id").collect()]
prod_preco    = {r.produto_id: r.preco     for r in df_produtos.select("produto_id","preco").collect()}
prod_cat      = {r.produto_id: r.categoria for r in df_produtos.select("produto_id","categoria").collect()}

vendas_rows = []
for i in range(1, 50001):
    pid        = random.choice(produto_ids)
    preco_base = prod_preco[pid]
    qtd        = random.randint(1, 5)
    desc       = random.choice([0, 0, 0, 5, 10, 15, 20]) / 100
    val_total  = round(preco_base * (1 - desc) * qtd, 2)
    dt         = fake.date_between(start_date=date(2022, 1, 1), end_date=date(2024, 12, 31))

    vendas_rows.append(Row(
        venda_id       = i,
        cliente_id     = random.choice(cliente_ids),
        produto_id     = pid,
        categoria      = prod_cat[pid],
        quantidade     = qtd,
        preco_unitario = preco_base,
        desconto_pct   = int(desc * 100),
        valor_total    = val_total,
        canal          = random.choices(canais, weights=canal_pesos)[0],
        pagamento      = random.choices(pagamentos, weights=pag_pesos)[0],
        status         = random.choice(status_lista),
        data_venda     = dt,
        ano            = dt.year,
        mes            = dt.month,
    ))

df_vendas = spark.createDataFrame(vendas_rows)
df_vendas.write.format("delta").mode("overwrite") \
    .partitionBy("ano", "mes") \
    .saveAsTable("certificacao_bronze.vendas")
print(f"✓ vendas: {df_vendas.count():,} registros (particionado por ano/mes)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Passo 7 — EVENTOS (20.000 registros com payload JSON)

# COMMAND ----------

tipos_evento  = ["login","logout","compra","busca","visualizacao",
                 "carrinho_add","carrinho_remove","checkout","erro_pagamento"]
tipo_pesos    = [0.20, 0.15, 0.15, 0.20, 0.15, 0.05, 0.03, 0.04, 0.03]
dispositivos  = ["desktop", "mobile", "tablet"]
disp_pesos    = [0.45, 0.40, 0.15]

eventos_rows = []
for i in range(1, 20001):
    tipo = random.choices(tipos_evento, weights=tipo_pesos)[0]
    cid  = random.choice(cliente_ids)
    ts   = fake.date_time_between(start_date=datetime(2024, 1, 1), end_date=datetime(2024, 12, 31))

    if tipo == "compra":
        payload = json.dumps({
            "produto_id": random.choice(produto_ids),
            "valor": round(random.uniform(50, 5000), 2),
            "pagamento": random.choices(pagamentos, weights=pag_pesos)[0],
        }, ensure_ascii=False)
    elif tipo == "busca":
        termos = ["notebook gamer","mouse sem fio","teclado mecânico","monitor 4k",
                  "fone bluetooth","ssd 1tb","webcam full hd","hub usb","headset"]
        payload = json.dumps({"termo": random.choice(termos),
                              "resultados": random.randint(0, 50)}, ensure_ascii=False)
    elif tipo in ("login", "logout"):
        payload = json.dumps({
            "ip": fake.ipv4(),
            "dispositivo": random.choices(dispositivos, weights=disp_pesos)[0],
            "browser": random.choice(["Chrome","Firefox","Safari","Edge"]),
        }, ensure_ascii=False)
    else:
        payload = json.dumps({"acao": tipo, "status": "ok"}, ensure_ascii=False)

    eventos_rows.append(Row(
        evento_id       = i,
        cliente_id      = cid,
        tipo_evento     = tipo,
        dispositivo     = random.choices(dispositivos, weights=disp_pesos)[0],
        payload         = payload,
        timestamp_evento= ts,
        data_evento     = ts.date(),
    ))

df_eventos = spark.createDataFrame(eventos_rows)
df_eventos.write.format("delta").mode("overwrite").saveAsTable("certificacao_bronze.eventos")
print(f"✓ eventos: {df_eventos.count():,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Passo 8 — FUNCIONARIOS (1.000 registros com dados sensíveis)

# COMMAND ----------

departamentos = {
    "Engenharia": (8000, 25000), "Dados": (9000, 28000), "Produto": (7500, 20000),
    "Marketing":  (5000, 15000), "Vendas":(4500, 12000), "RH":     (4000, 10000),
    "Financeiro": (5500, 16000), "Jurídico":(8000,22000),"Operações":(4500,14000),
    "TI":         (6000, 18000),
}
niveis       = ["Júnior","Pleno","Sênior","Especialista","Coordenador","Gerente","Diretor"]
nivel_pesos  = [0.20, 0.25, 0.25, 0.15, 0.08, 0.05, 0.02]
nivel_mult   = {"Júnior":0.6,"Pleno":0.8,"Sênior":1.0,"Especialista":1.2,
                "Coordenador":1.4,"Gerente":1.7,"Diretor":2.2}

func_rows = []
for i in range(1, 1001):
    dept   = random.choice(list(departamentos.keys()))
    smin, smax = departamentos[dept]
    nivel  = random.choices(niveis, weights=nivel_pesos)[0]
    sal    = round(random.uniform(smin, smax) * nivel_mult[nivel], 2)
    dt_adm = fake.date_between(start_date=date(2015, 1, 1), end_date=date(2024, 6, 30))

    func_rows.append(Row(
        funcionario_id   = i,
        nome             = fake.name(),
        cpf              = fake.cpf(),
        email_corporativo= fake.company_email(),
        departamento     = dept,
        cargo            = f"{nivel} de {dept}",
        nivel            = nivel,
        salario          = sal,
        estado           = random.choice(list(estados_capitais.keys())),
        data_admissao    = dt_adm,
        gestor_id        = random.randint(1, 100) if nivel not in ("Diretor","Gerente") else None,
        ativo            = random.random() > 0.10,
    ))

df_func = spark.createDataFrame(func_rows)
df_func.write.format("delta").mode("overwrite").saveAsTable("certificacao_bronze.funcionarios")
print(f"✓ funcionarios: {df_func.count():,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Passo 9 — VENDAS_RAW (dados sujos para exercícios de ELT)
# MAGIC
# MAGIC Problemas introduzidos propositalmente:
# MAGIC - 5% produto nulo
# MAGIC - 5% valor negativo
# MAGIC - 5% estado nulo
# MAGIC - 5% data no formato errado (dd/mm/yyyy)
# MAGIC - ~10% registros duplicados

# COMMAND ----------

sample = df_vendas.limit(5000).collect()
raw_rows = []

for i, v in enumerate(sample):
    r = random.random()
    prod_nome = f"Produto-{v.produto_id}"
    dt_str    = str(v.data_venda)

    if r < 0.05:                          # produto nulo
        raw_rows.append(Row(id=i+1, produto=None, categoria=v.categoria,
                            valor=str(v.valor_total), data_venda=dt_str,
                            estado=random.choice(list(estados_capitais.keys())),
                            canal=v.canal, status=v.status))
    elif r < 0.10:                        # valor negativo
        raw_rows.append(Row(id=i+1, produto=prod_nome, categoria=v.categoria,
                            valor=str(-abs(v.valor_total)), data_venda=dt_str,
                            estado=random.choice(list(estados_capitais.keys())),
                            canal=v.canal, status=v.status))
    elif r < 0.15:                        # estado nulo
        raw_rows.append(Row(id=i+1, produto=prod_nome, categoria=v.categoria,
                            valor=str(v.valor_total), data_venda=dt_str,
                            estado=None, canal=v.canal, status=v.status))
    elif r < 0.20:                        # data malformada
        raw_rows.append(Row(id=i+1, produto=prod_nome, categoria=v.categoria,
                            valor=str(v.valor_total),
                            data_venda=fake.date(pattern="%d/%m/%Y"),
                            estado=random.choice(list(estados_capitais.keys())),
                            canal=v.canal, status=v.status))
    else:                                 # registro normal
        raw_rows.append(Row(id=i+1, produto=prod_nome, categoria=v.categoria,
                            valor=str(v.valor_total), data_venda=dt_str,
                            estado=random.choice(list(estados_capitais.keys())),
                            canal=v.canal, status=v.status))

# Adicionar ~500 duplicatas
dups = random.sample(raw_rows, k=500)
raw_final = raw_rows + dups
random.shuffle(raw_final)
raw_final = [Row(id=idx+1, produto=r.produto, categoria=r.categoria,
                 valor=r.valor, data_venda=r.data_venda,
                 estado=r.estado, canal=r.canal, status=r.status)
             for idx, r in enumerate(raw_final)]

df_raw = spark.createDataFrame(raw_final)
df_raw.write.format("delta").mode("overwrite").saveAsTable("certificacao_bronze.vendas_raw")
print(f"✓ vendas_raw: {df_raw.count():,} registros (com nulos, negativos, duplicatas, datas ruins)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Passo 10 — CSVs Incrementais para Auto Loader (3 lotes)

# COMMAND ----------

import pandas as pd

base_path = "/tmp/autoloader_vendas"
try:
    dbutils.fs.rm(base_path, recurse=True)
except:
    pass
dbutils.fs.mkdirs(base_path)

for lote in range(1, 4):
    lote_data = []
    for j in range(150):
        pid   = random.choice(produto_ids)
        dt_v  = fake.date_between(
            start_date=date(2025, lote, 1),
            end_date=date(2025, lote, 28)
        )
        lote_data.append({
            "venda_id"  : 50000 + (lote - 1) * 150 + j + 1,
            "cliente_id": random.choice(cliente_ids),
            "produto_id": pid,
            "categoria" : prod_cat[pid],
            "quantidade": random.randint(1, 3),
            "valor_total": round(prod_preco[pid] * random.randint(1, 3), 2),
            "canal"     : random.choices(canais, weights=canal_pesos)[0],
            "pagamento" : random.choices(pagamentos, weights=pag_pesos)[0],
            "status"    : random.choice(["concluida","concluida","concluida","cancelada"]),
            "data_venda": str(dt_v),
        })

    (spark.createDataFrame(pd.DataFrame(lote_data))
          .coalesce(1)
          .write.format("csv")
          .option("header", "true")
          .mode("overwrite")
          .save(f"{base_path}/lote_{lote:02d}"))

    print(f"  ✓ Lote {lote}: 150 registros → {base_path}/lote_{lote:02d}/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Passo 11 — Verificação Final

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'clientes'      AS tabela, COUNT(*) AS registros FROM certificacao_bronze.clientes
# MAGIC UNION ALL SELECT 'produtos',      COUNT(*) FROM certificacao_bronze.produtos
# MAGIC UNION ALL SELECT 'vendas',        COUNT(*) FROM certificacao_bronze.vendas
# MAGIC UNION ALL SELECT 'eventos',       COUNT(*) FROM certificacao_bronze.eventos
# MAGIC UNION ALL SELECT 'funcionarios',  COUNT(*) FROM certificacao_bronze.funcionarios
# MAGIC UNION ALL SELECT 'vendas_raw',    COUNT(*) FROM certificacao_bronze.vendas_raw
# MAGIC ORDER BY registros DESC;

# COMMAND ----------

print("=" * 60)
print("DADOS GERADOS COM SUCESSO!")
print("=" * 60)
print()
print("Tabelas Delta em certificacao_bronze:")
print("  clientes     →  5.000 | nome, CPF, email, plano, estado")
print("  produtos     →    500 | categoria, marca, preço, margem")
print("  vendas       → 50.000 | particionado por ano/mes")
print("  eventos      → 20.000 | logs JSON (login, compra, busca...)")
print("  funcionarios →  1.000 | dados sensíveis (CPF, salário)")
print("  vendas_raw   →  ~5.500 | dados SUJOS para limpeza")
print()
print("CSVs em /tmp/autoloader_vendas/:")
print("  lote_01/ → 150 registros (Jan/2025)")
print("  lote_02/ → 150 registros (Fev/2025)")
print("  lote_03/ → 150 registros (Mar/2025)")
print()
print("Próximo passo: modulo-01-fundamentos/ !")
print("=" * 60)
