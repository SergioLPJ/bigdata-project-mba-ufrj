from flask import Flask, render_template, request
from databricks import sql
import pandas as pd
import time

app = Flask(__name__)

# ----------------------------
# Configuração Databricks
# ----------------------------
DATABRICKS_SERVER_HOSTNAME = "seu_host_aqui"
DATABRICKS_HTTP_PATH = "seu_path_aqui"
DATABRICKS_TOKEN = "seu_token_aqui"

# ----------------------------
# Página inicial
# ----------------------------
@app.route('/')
def index():
    return render_template('index.html', resultados=[], linha='')

# ----------------------------
# Formulário de busca
# ----------------------------
@app.route('/buscar', methods=['POST'])
def buscar():
    linha = request.form.get('linha', '').strip().lower()  # normaliza entrada
    limite = 300  # limite máximo de registros

    resultados = []
    error = None

    try:
        with sql.connect(
            server_hostname=DATABRICKS_SERVER_HOSTNAME,
            http_path=DATABRICKS_HTTP_PATH,
            access_token=DATABRICKS_TOKEN
        ) as conn:
            with conn.cursor() as cursor:
                # 1. Buscar todas as tabelas
                cursor.execute("SHOW TABLES")
                tables = cursor.fetchall()
                colunas_tabelas = [desc[0] for desc in cursor.description]
                df_tables = pd.DataFrame(tables, columns=colunas_tabelas)

                # 2. Filtrar tabelas do padrão 'lotacao_onibus_teresina'
                df_tables = df_tables[df_tables['tableName'].str.startswith("lotacao_onibus_teresina")]

                if df_tables.empty:
                    error = "Nenhuma tabela encontrada no Databricks."
                else:
                    # 🔹 Pegar a tabela mais recente pelo timestamp no nome
                    df_tables['timestamp'] = df_tables['tableName'].str.extract(r'(\d{8}_\d{6})')[0]
                    df_tables = df_tables.dropna(subset=['timestamp'])
                    df_tables = df_tables.sort_values('timestamp', ascending=False)
                    ultima_tabela = df_tables.iloc[0]['tableName']
                    print(f"Última tabela selecionada: {ultima_tabela}")

                    # 3. Montar query de busca
                    query = f"""
                        SELECT *
                        FROM {ultima_tabela}
                        WHERE lower(linha) LIKE '%{linha}%'
                        LIMIT {limite}
                    """
                    print("Query executada:", query)

                    # 4. Executar query
                    cursor.execute(query)
                    rows = cursor.fetchall()
                    colunas = [desc[0] for desc in cursor.description]
                    resultados = pd.DataFrame(rows, columns=colunas).to_dict(orient="records")
                    print(f"Total de resultados encontrados: {len(resultados)}")

    except Exception as e:
        error = f"Erro na consulta: {str(e)}"
        print(error)

    return render_template(
        'index.html',
        resultados=resultados,
        linha=linha,
        error=error
    )

# ----------------------------
# Executa Flask localmente
# ----------------------------
if __name__ == "__main__":
    app.run(debug=True)
