import pandas as pd
import os
import time
import base64
from databricks_api import DatabricksAPI
import certifi

# ------------------------
# CONFIGURAÇÃO DE TESTE
# ------------------------
LIMIT_RECORDS = 800  # 🔹 limite final de registros
JOB_ID = seu_id_aqui

# ------------------------
# Configuração Databricks
# ------------------------
databricks = DatabricksAPI(
    host="seu_host_aqui",
    token="seu_token_aqui"  # substitua pelo token real
)
databricks.client.session.verify = certifi.where()

# ------------------------
# Caminhos locais
# ------------------------
GTFS_DIR = r"H:\Meu Drive\Con9soft\PROJETOS_VSCODE\PROJETO_PYTHON\gtfs_rj"
TMP_DIR = os.path.join(GTFS_DIR, "tmp")
os.makedirs(TMP_DIR, exist_ok=True)

trips_path = os.path.join(GTFS_DIR, "trips.txt")
stop_times_path = os.path.join(GTFS_DIR, "stop_times.txt")
local_json_path = os.path.join(TMP_DIR, "dados_gtfs.json")

# ------------------------
# Lê arquivos CSV COMPLETOS
# ------------------------
trips_df = pd.read_csv(trips_path, dtype={"trip_id": str})
stop_times_df = pd.read_csv(stop_times_path, dtype={"trip_id": str})

# ------------------------
# Processamento (merge completo)
# ------------------------
pontos_df = stop_times_df.groupby("trip_id").size().reset_index(name="pontos_parada")
merged = trips_df.merge(pontos_df, on="trip_id", how="left")
merged["pontos_parada"] = merged["pontos_parada"].fillna(0).astype(int)
merged["capacidade"] = 40
merged = merged[["route_id", "trip_id", "pontos_parada", "capacidade"]].rename(columns={"route_id": "linha"})

# ------------------------
# Aplica limite final de registros
# ------------------------
merged = merged.head(LIMIT_RECORDS)

# ------------------------
# Converte para JSON (compacto) e salva local (backup)
# ------------------------
dados_json = merged.to_json(orient="records", force_ascii=False)
with open(local_json_path, "w", encoding="utf-8") as f:
    f.write(dados_json)

print(f"JSON salvo localmente em: {local_json_path}")
print(f"Tamanho do JSON: {len(dados_json)} bytes")

# ------------------------
# Decide envio: direto ou via DBFS
# ------------------------
if len(dados_json.encode("utf-8")) <= 10000:
    notebook_params = {
        "linha": "",
        "limite": str(LIMIT_RECORDS),
        "dados_json": dados_json
    }
    print("Enviando JSON direto (≤10 KB)")
else:
    dbfs_path = "dbfs:/tmp/dados_gtfs.json"
    with open(local_json_path, "rb") as f:
        file_bytes = f.read()
    file_b64 = base64.b64encode(file_bytes).decode("utf-8")
    databricks.dbfs.put(dbfs_path, file_b64, overwrite=True)
    notebook_params = {
        "linha": "",
        "limite": str(LIMIT_RECORDS),
        "dbfs_json_path": dbfs_path
    }
    print(f"JSON grande (>10 KB). Salvo no DBFS: {dbfs_path}")

# ------------------------
# Chama o notebook no Databricks
# ------------------------
run = databricks.jobs.run_now(
    job_id=JOB_ID,
    notebook_params=notebook_params
)

run_id = run["run_id"]
print(f"Job iniciado com run_id={run_id}")

# ------------------------
# Espera o job terminar
# ------------------------
while True:
    status = databricks.jobs.get_run(run_id=run_id)
    state = status["state"]["life_cycle_state"]
    print("Status atual:", state)
    if state in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
        break
    time.sleep(2)

# ------------------------
# Recupera resultado
# ------------------------
output = databricks.jobs.get_run_output(run_id=run_id)