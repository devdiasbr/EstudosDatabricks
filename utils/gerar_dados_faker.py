# Databricks notebook source

# MAGIC %md
# MAGIC # Gerador de Dados com Faker
# MAGIC
# MAGIC **Execute este notebook APÓS o `setup_databases.py`.**
# MAGIC
# MAGIC Gera dados realistas para todos os módulos usando a biblioteca Faker.
# MAGIC
# MAGIC **Tabelas geradas:**
# MAGIC | Tabela | Registros | Usado nos módulos |
# MAGIC |--------|-----------|-------------------|
# MAGIC | `certificacao_bronze.clientes` | 5.000 | 3, 4, 6, 7, 8 |
# MAGIC | `certificacao_bronze.produtos` | 500 | 3, 4, 5 |
# MAGIC | `certificacao_bronze.vendas` | 50.000 | 3, 4, 5, 7, 8 |
# MAGIC | `certificacao_bronze.eventos` | 20.000 | 5, 6 |
# MAGIC | `certificacao_bronze.funcionarios` | 1.000 | 6 (Dynamic Views) |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Instalar Faker

# COMMAND ----------

# MAGIC %pip install faker

# COMMAND ----------

# Reiniciar Python após instalação
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports e Setup

# COMMAND ----------

from faker import Faker
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, DateType, TimestampType, BooleanType, ArrayType
)
from pyspark.sql.functions import (
    col, lit, expr, rand, when, round as spark_round,
    date_add, current_timestamp, concat, array, element_at
)
from datetime import datetime, timedelta
import random

fake = Faker("pt_BR")
Faker.seed(42)
random.seed(42)

print("Faker configurado com locale pt_BR")
print(f"Exemplo de nome: {fake.name()}")
print(f"Exemplo de cidade: {fake.city()}")
print(f"Exemplo de empresa: {fake.company()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. Tabela: CLIENTES (5.000 registros)

# COMMAND ----------

def gerar_clientes(n=5000):
    """Gera dados de clientes brasileiros."""
    estados = ["SP", "RJ", "MG", "RS", "PR", "SC", "BA", "PE", "CE", "DF",
               "GO", "PA", "MA", "MT", "MS", "ES", "PB", "RN", "AL", "SE"]
    planos = ["free", "basic", "premium", "enterprise"]

    dados = []
    for i in range(1, n + 1):
        estado = random.choice(estados)
        dados.append((
            i,
            fake.name(),
            fake.email(),
            fake.cpf(),
            fake.phone_number(),
            fake.street_address(),
            fake.city(),
            estado,
            fake.postcode(),
            random.choice(planos),
            fake.date_between(start_date="-3y", end_date="today"),
            random.choice([True, True, True, False]),  # 75% ativos
            random.randint(18, 75)
        ))

    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("nome", StringType(), True),
        StructField("email", StringType(), True),
        StructField("cpf", StringType(), True),
        StructField("telefone", StringType(), True),
        StructField("endereco", StringType(), True),
        StructField("cidade", StringType(), True),
        StructField("estado", StringType(), True),
        StructField("cep", StringType(), True),
        StructField("plano", StringType(), True),
        StructField("data_cadastro", DateType(), True),
        StructField("ativo", BooleanType(), True),
        StructField("idade", IntegerType(), True)
    ])

    return spark.createDataFrame(dados, schema)

# Gerar e salvar
df_clientes = gerar_clientes(5000)
df_clientes.write.format("delta").mode("overwrite").saveAsTable("certificacao_bronze.clientes")
print(f"clientes: {df_clientes.count()} registros salvos")
display(df_clientes.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. Tabela: PRODUTOS (500 registros)

# COMMAND ----------

def gerar_produtos(n=500):
    """Gera catálogo de produtos."""
    categorias = {
        "eletronicos": ["Notebook", "Smartphone", "Tablet", "Smart TV", "Câmera Digital",
                        "Console", "Drone", "Smartwatch", "Caixa de Som", "Projetor"],
        "perifericos": ["Mouse", "Teclado", "Headset", "Webcam", "Monitor",
                        "Hub USB", "Mousepad", "Suporte Monitor", "Cabo HDMI", "Adaptador"],
        "armazenamento": ["SSD 500GB", "SSD 1TB", "SSD 2TB", "HD Externo 1TB", "HD Externo 2TB",
                          "Pen Drive 64GB", "Pen Drive 128GB", "Cartão SD 256GB", "NAS 4TB", "Fita LTO"],
        "software": ["Office 365", "Antivírus", "Adobe Creative", "AutoCAD", "Windows Pro",
                     "Linux Enterprise", "Backup Cloud", "VPN Pro", "IDE License", "DB License"],
        "moveis": ["Cadeira Gamer", "Mesa Standing", "Estante", "Gaveteiro", "Luminária LED",
                   "Apoio Pés", "Organizador Cabos", "Suporte Notebook", "Quadro Branco", "Cortina Blackout"]
    }

    marcas = ["TechPro", "DataMax", "CloudBit", "ByteForce", "NeoTech",
              "SmartEdge", "CoreLink", "PixelWave", "InfoPrime", "CyberNode"]

    dados = []
    id_counter = 1
    for categoria, produtos in categorias.items():
        for produto_base in produtos:
            for _ in range(n // (len(categorias) * len(produtos)) + 1):
                if id_counter > n:
                    break
                marca = random.choice(marcas)
                nome = f"{produto_base} {marca} {fake.bothify('??-####').upper()}"

                faixas_preco = {
                    "eletronicos": (800, 8000),
                    "perifericos": (30, 1500),
                    "armazenamento": (50, 2000),
                    "software": (100, 5000),
                    "moveis": (150, 3000)
                }
                preco_min, preco_max = faixas_preco[categoria]

                dados.append((
                    id_counter,
                    nome,
                    categoria,
                    marca,
                    round(random.uniform(preco_min, preco_max), 2),
                    round(random.uniform(preco_min * 0.5, preco_min * 0.9), 2),
                    random.randint(0, 500),
                    random.choice([True, True, True, True, False]),
                    round(random.uniform(0, 5), 1),
                    fake.date_between(start_date="-2y", end_date="today")
                ))
                id_counter += 1
            if id_counter > n:
                break
        if id_counter > n:
            break

    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("nome", StringType(), True),
        StructField("categoria", StringType(), True),
        StructField("marca", StringType(), True),
        StructField("preco", DoubleType(), True),
        StructField("custo", DoubleType(), True),
        StructField("estoque", IntegerType(), True),
        StructField("ativo", BooleanType(), True),
        StructField("avaliacao", DoubleType(), True),
        StructField("data_cadastro", DateType(), True)
    ])

    return spark.createDataFrame(dados[:n], schema)

df_produtos = gerar_produtos(500)
df_produtos.write.format("delta").mode("overwrite").saveAsTable("certificacao_bronze.produtos")
print(f"produtos: {df_produtos.count()} registros salvos")
display(df_produtos.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. Tabela: VENDAS (50.000 registros)

# COMMAND ----------

def gerar_vendas(n=50000, max_cliente=5000, max_produto=500):
    """Gera transações de vendas."""
    canais = ["site", "app", "loja_fisica", "marketplace", "televendas"]
    status_list = ["concluida", "concluida", "concluida", "concluida",
                   "cancelada", "devolvida", "pendente"]
    formas_pgto = ["pix", "credito", "debito", "boleto", "credito_parcelado"]

    dados = []
    for i in range(1, n + 1):
        data_venda = fake.date_between(start_date="-2y", end_date="today")
        status = random.choice(status_list)
        quantidade = random.randint(1, 5)
        preco_unit = round(random.uniform(30, 5000), 2)
        desconto = round(random.uniform(0, 0.2), 2) if random.random() > 0.6 else 0.0

        dados.append((
            i,
            random.randint(1, max_cliente),
            random.randint(1, max_produto),
            data_venda,
            quantidade,
            preco_unit,
            desconto,
            round(preco_unit * quantidade * (1 - desconto), 2),
            random.choice(canais),
            random.choice(formas_pgto),
            status,
            random.choice(["SP", "RJ", "MG", "RS", "PR", "SC", "BA", "PE", "CE", "DF"]),
            fake.bothify("PED-########").upper()
        ))

    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("cliente_id", IntegerType(), True),
        StructField("produto_id", IntegerType(), True),
        StructField("data_venda", DateType(), True),
        StructField("quantidade", IntegerType(), True),
        StructField("preco_unitario", DoubleType(), True),
        StructField("desconto", DoubleType(), True),
        StructField("valor_total", DoubleType(), True),
        StructField("canal", StringType(), True),
        StructField("forma_pagamento", StringType(), True),
        StructField("status", StringType(), True),
        StructField("estado", StringType(), True),
        StructField("codigo_pedido", StringType(), True)
    ])

    return spark.createDataFrame(dados, schema)

df_vendas = gerar_vendas(50000)
df_vendas.write.format("delta").mode("overwrite").saveAsTable("certificacao_bronze.vendas")
print(f"vendas: {df_vendas.count()} registros salvos")
display(df_vendas.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. Tabela: EVENTOS (20.000 registros)

# COMMAND ----------

def gerar_eventos(n=20000, max_cliente=5000):
    """Gera eventos de sistema (logs de atividade)."""
    tipos_evento = [
        "login", "logout", "page_view", "add_to_cart", "purchase",
        "search", "click", "signup", "password_reset", "profile_update",
        "payment_failed", "review_submitted", "coupon_applied", "wishlist_add"
    ]

    plataformas = ["web", "ios", "android", "api"]
    browsers = ["Chrome", "Firefox", "Safari", "Edge", None]

    dados = []
    for i in range(1, n + 1):
        evento = random.choice(tipos_evento)
        ts = fake.date_time_between(start_date="-6M", end_date="now")

        # Payload JSON como string
        payload_map = {
            "login": f'{{"ip": "{fake.ipv4()}", "success": {str(random.choice([True, True, True, False])).lower()}}}',
            "page_view": f'{{"page": "/{fake.uri_path()}", "duration_ms": {random.randint(500, 30000)}}}',
            "purchase": f'{{"order_id": "PED-{random.randint(10000,99999)}", "valor": {round(random.uniform(50, 5000), 2)}}}',
            "search": f'{{"query": "{fake.word()}", "results": {random.randint(0, 200)}}}',
            "add_to_cart": f'{{"produto_id": {random.randint(1, 500)}, "quantidade": {random.randint(1, 3)}}}',
        }
        payload = payload_map.get(evento, f'{{"detail": "{fake.sentence(nb_words=3)}"}}')

        dados.append((
            i,
            random.randint(1, max_cliente) if random.random() > 0.1 else None,  # 10% anônimo
            evento,
            ts,
            random.choice(plataformas),
            random.choice(browsers),
            fake.ipv4() if random.random() > 0.2 else None,
            fake.user_agent() if random.random() > 0.5 else None,
            payload
        ))

    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("cliente_id", IntegerType(), True),
        StructField("tipo_evento", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("plataforma", StringType(), True),
        StructField("browser", StringType(), True),
        StructField("ip", StringType(), True),
        StructField("user_agent", StringType(), True),
        StructField("payload", StringType(), True)
    ])

    return spark.createDataFrame(dados, schema)

df_eventos = gerar_eventos(20000)
df_eventos.write.format("delta").mode("overwrite").saveAsTable("certificacao_bronze.eventos")
print(f"eventos: {df_eventos.count()} registros salvos")
display(df_eventos.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5. Tabela: FUNCIONÁRIOS (1.000 registros)
# MAGIC
# MAGIC Usada no Módulo 6 para exercícios de Dynamic Views (mascaramento de dados).

# COMMAND ----------

def gerar_funcionarios(n=1000):
    """Gera dados de funcionários (com dados sensíveis para exercícios de governança)."""
    departamentos = ["Engenharia", "Dados", "Produto", "Marketing", "Vendas",
                     "RH", "Financeiro", "Suporte", "Jurídico", "Operações"]
    cargos = {
        "Engenharia": ["Dev Junior", "Dev Pleno", "Dev Senior", "Tech Lead", "Staff Engineer"],
        "Dados": ["Analista Dados Jr", "Analista Dados Pl", "Data Engineer", "Data Engineer Sr", "Head of Data"],
        "Produto": ["PM Junior", "PM Pleno", "PM Senior", "GPM", "CPO"],
        "Marketing": ["Analista Mkt", "Coordenador Mkt", "Gerente Mkt", "CMO", "Designer"],
        "Vendas": ["SDR", "AE Junior", "AE Senior", "Sales Manager", "VP Sales"],
        "RH": ["Analista RH", "BP", "Recrutador", "Head RH", "CHRO"],
        "Financeiro": ["Analista Fin", "Controller", "Tesoureiro", "CFO", "Contador"],
        "Suporte": ["Suporte N1", "Suporte N2", "Suporte N3", "Coordenador", "Head Suporte"],
        "Jurídico": ["Advogado Jr", "Advogado Pl", "Advogado Sr", "Head Legal", "CLO"],
        "Operações": ["Analista Ops", "Coordenador Ops", "Gerente Ops", "COO", "SRE"]
    }
    niveis_acesso = ["viewer", "editor", "admin"]

    dados = []
    for i in range(1, n + 1):
        dept = random.choice(departamentos)
        cargo = random.choice(cargos[dept])

        # Salário baseado no cargo (posição na lista = senioridade)
        idx = cargos[dept].index(cargo)
        base = 3000 + (idx * 3000)
        salario = round(base + random.uniform(-500, 2000), 2)

        dados.append((
            i,
            fake.name(),
            fake.email(),
            fake.cpf(),
            dept,
            cargo,
            salario,
            random.choice(niveis_acesso),
            fake.date_between(start_date="-5y", end_date="-30d"),
            random.choice([True, True, True, True, False]),
            fake.phone_number(),
            fake.date_of_birth(minimum_age=22, maximum_age=60)
        ))

    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("nome", StringType(), True),
        StructField("email", StringType(), True),
        StructField("cpf", StringType(), True),
        StructField("departamento", StringType(), True),
        StructField("cargo", StringType(), True),
        StructField("salario", DoubleType(), True),
        StructField("nivel_acesso", StringType(), True),
        StructField("data_admissao", DateType(), True),
        StructField("ativo", BooleanType(), True),
        StructField("telefone", StringType(), True),
        StructField("data_nascimento", DateType(), True)
    ])

    return spark.createDataFrame(dados, schema)

df_funcionarios = gerar_funcionarios(1000)
df_funcionarios.write.format("delta").mode("overwrite").saveAsTable("certificacao_bronze.funcionarios")
print(f"funcionarios: {df_funcionarios.count()} registros salvos")
display(df_funcionarios.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 6. Dados "sujos" para exercícios de limpeza (Silver)
# MAGIC
# MAGIC Versão propositalmente com problemas para praticar ELT no Módulo 3.

# COMMAND ----------

def gerar_vendas_sujas(n=5000):
    """Gera dados com problemas reais: nulos, duplicatas, valores inválidos."""
    dados_limpos = []

    for i in range(1, n + 1):
        # Introduzir problemas propositalmente
        nome_produto = fake.word().title() if random.random() > 0.05 else None  # 5% nulo
        valor = round(random.uniform(-100, 5000), 2)  # ~2% negativos
        estado = random.choice(["SP", "RJ", "MG", "sp", "rj", " SP ", None])  # inconsistências
        data = fake.date_between(start_date="-1y", end_date="today").isoformat() if random.random() > 0.03 else "INVALID"
        email = fake.email() if random.random() > 0.08 else "email_invalido"

        dados_limpos.append((
            i,
            nome_produto,
            random.choice(["eletronicos", "ELETRONICOS", "Eletronicos", "perifericos", "PERIFERICOS", None]),
            str(valor),  # valor como string (precisa converter)
            data,
            estado,
            email,
            random.randint(1, 5000)
        ))

    # Adicionar duplicatas (10% dos registros)
    duplicatas = random.sample(dados_limpos, n // 10)
    dados_limpos.extend(duplicatas)
    random.shuffle(dados_limpos)

    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("produto", StringType(), True),
        StructField("categoria", StringType(), True),
        StructField("valor", StringType(), True),  # propositalmente string
        StructField("data_venda", StringType(), True),  # propositalmente string
        StructField("estado", StringType(), True),
        StructField("email_cliente", StringType(), True),
        StructField("cliente_id", IntegerType(), True)
    ])

    return spark.createDataFrame(dados_limpos, schema)

df_sujo = gerar_vendas_sujas(5000)
df_sujo.write.format("delta").mode("overwrite").saveAsTable("certificacao_bronze.vendas_raw")
print(f"vendas_raw (dados sujos): {df_sujo.count()} registros salvos")
print(f"  - Inclui: nulos, duplicatas, valores negativos, tipos errados, inconsistências")
display(df_sujo.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 7. Dados incrementais para exercícios de Streaming (Módulo 5)

# COMMAND ----------

# Salvar CSVs incrementais para simular Auto Loader
import json

lotes = [
    ("lote_01", 100, "-30d", "-20d"),
    ("lote_02", 150, "-20d", "-10d"),
    ("lote_03", 200, "-10d", "today"),
]

for nome_lote, qtd, inicio, fim in lotes:
    dados = []
    for i in range(qtd):
        dados.append((
            fake.bothify("PED-########"),
            random.randint(1, 5000),
            random.randint(1, 500),
            random.randint(1, 5),
            round(random.uniform(30, 3000), 2),
            fake.date_between(start_date=inicio, end_date=fim).isoformat()
        ))

    df_lote = spark.createDataFrame(dados,
        ["codigo_pedido", "cliente_id", "produto_id", "quantidade", "valor", "data_venda"])

    path = f"/tmp/certificacao/streaming/vendas/{nome_lote}"
    df_lote.write.format("csv").mode("overwrite").option("header", "true").save(path)
    print(f"  {nome_lote}: {qtd} registros salvos em {path}")

print("\nDados incrementais prontos para Auto Loader!")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Resumo dos Dados Gerados

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'clientes' as tabela, COUNT(*) as registros FROM certificacao_bronze.clientes
# MAGIC UNION ALL SELECT 'produtos', COUNT(*) FROM certificacao_bronze.produtos
# MAGIC UNION ALL SELECT 'vendas', COUNT(*) FROM certificacao_bronze.vendas
# MAGIC UNION ALL SELECT 'eventos', COUNT(*) FROM certificacao_bronze.eventos
# MAGIC UNION ALL SELECT 'funcionarios', COUNT(*) FROM certificacao_bronze.funcionarios
# MAGIC UNION ALL SELECT 'vendas_raw (sujo)', COUNT(*) FROM certificacao_bronze.vendas_raw
# MAGIC ORDER BY registros DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificar qualidade dos dados

# COMMAND ----------

# Estatísticas rápidas
for tabela in ["clientes", "produtos", "vendas", "eventos", "funcionarios"]:
    df = spark.table(f"certificacao_bronze.{tabela}")
    print(f"\n{'='*50}")
    print(f"certificacao_bronze.{tabela}")
    print(f"  Registros: {df.count():,}")
    print(f"  Colunas: {len(df.columns)}")
    print(f"  Colunas: {', '.join(df.columns)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Checkpoint
# MAGIC
# MAGIC - [ ] Faker instalado com `%pip install faker`
# MAGIC - [ ] 5.000 clientes gerados (com CPF, endereço, plano)
# MAGIC - [ ] 500 produtos gerados (5 categorias, 10 marcas)
# MAGIC - [ ] 50.000 vendas geradas (com canal, pagamento, status)
# MAGIC - [ ] 20.000 eventos gerados (com payload JSON)
# MAGIC - [ ] 1.000 funcionários gerados (com dados sensíveis para governança)
# MAGIC - [ ] Dados sujos gerados para exercícios de limpeza
# MAGIC - [ ] CSVs incrementais para Auto Loader
# MAGIC
# MAGIC **Pronto! Agora você tem dados realistas para todos os exercícios do curso.**
