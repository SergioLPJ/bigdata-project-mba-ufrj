from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lower
from pyspark.sql.types import ArrayType, IntegerType
import random
import pandas as pd
import io
import time
import uuid
from datetime import datetime, timezone, timedelta

# ------------------------
# Inicializa Spark
# ------------------------
spark = SparkSession.builder.getOrCreate()

# ------------------------
# Widgets / Parâmetros
# ------------------------
dbutils.widgets.text("linha", "", "Linha")
dbutils.widgets.text("limite", "10", "Limite")
dbutils.widgets.text("dados_json", "[]", "Dados JSON")
dbutils.widgets.text("dbfs_json_path", "", "DBFS JSON Path")

linha_param = dbutils.widgets.get("linha").strip()
limite = int(dbutils.widgets.get("limite"))
dbfs_json_path = dbutils.widgets.get("dbfs_json_path").strip()
dados_json = dbutils.widgets.get("dados_json").strip()

# ------------------------
# Lê JSON (DBFS se enviado, senão direto)
# ------------------------
if dbfs_json_path:
    print("Lendo JSON do DBFS:", dbfs_json_path)
    dados_json = dbutils.fs.head(dbfs_json_path, 1000000)  # até 1MB
else:
    print("Usando JSON direto do parâmetro")

print("Tamanho do JSON recebido:", len(dados_json))

# ------------------------
# Criação do DataFrame (sem schema fixo)
# ------------------------
if dados_json in ["", "[]"]:
    print("JSON vazio recebido!")
    df = spark.createDataFrame([], "linha STRING, trip_id STRING, pontos_parada INT, capacidade INT")
else:
    try:
        pdf = pd.read_json(io.StringIO(dados_json))
        print("Prévia do DataFrame Pandas:")
        print(pdf.head())
        df = spark.createDataFrame(pdf)
    except Exception as e:
        print("Erro ao converter JSON -> Pandas -> Spark:", str(e))
        df = spark.createDataFrame([], "linha STRING, trip_id STRING, pontos_parada INT, capacidade INT")

print("Linhas carregadas no DataFrame:", df.count())
df.printSchema()
df.show(5, truncate=False)

# ------------------------
# UDF - Simulação de Lotação
# ------------------------
def simula_lotacao(capacidade, pontos):
    if capacidade is None or pontos is None:
        return []
    lotacao = 0
    lotacao_por_ponto = []
    for i in range(pontos):
        embarque = random.randint(0, int(capacidade * 0.3))
        desembarque = random.randint(0, int(lotacao * 0.3))
        lotacao = min(capacidade, lotacao + embarque - desembarque)
        lotacao_por_ponto.append(lotacao)
    return lotacao_por_ponto

simula_udf = udf(simula_lotacao, ArrayType(IntegerType()))

if df.count() > 0:
    df = df.withColumn("lotacao_por_ponto", simula_udf(col("capacidade"), col("pontos_parada")))

# ------------------------
# Filtro e limite
# ------------------------
if df.count() > 0 and linha_param:
    df_filtered = df.filter(lower(col("linha")).like(f"%{linha_param.lower()}%")).limit(limite)
else:
    df_filtered = df.limit(limite)

print("Linhas após filtro:", df_filtered.count())
df_filtered.show(10, truncate=False)

# ------------------------
# Gravação na Delta Table com timestamp Brasil + UUID
# ------------------------
fuso_brasil = timezone(timedelta(hours=-3))
agora_brasil = datetime.now(fuso_brasil)
timestamp = agora_brasil.strftime("%Y%m%d_%H%M%S")  # AAAAMMDD_HHMMSS

nome_tabela = f"lotacao_onibus_teresina_{timestamp}_{uuid.uuid4().hex[:4]}"  # UUID curto para unicidade

print("Database ativo:", spark.catalog.currentDatabase())
print("Nome da Delta Table que será criada:", nome_tabela)

if df_filtered.count() > 0:
    df_filtered.write.format("delta").mode("overwrite").saveAsTable(nome_tabela)
    print(f"Delta Table '{nome_tabela}' criada com sucesso!")
else:
    print("Nenhum dado válido para salvar na Delta Table.")

# ------------------------
# Mostrar tabelas no schema atual
# ------------------------

#spark.sql(f"SHOW TABLES IN {spark.catalog.currentDatabase()}").show(truncate=False)

print("Tabelas atuais no schema:")

# Pega o banco atual e garante que está entre crases
db = spark.catalog.currentDatabase()
spark.sql(f"SHOW TABLES IN `{db}`").show(truncate=False)

# ------------------------
# Retorno em JSON
# ------------------------
resultado = df_filtered.toPandas().to_json(orient="records", force_ascii=False)
dbutils.notebook.exit(resultado)
