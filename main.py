import requests
import psycopg2
import time
from datetime import datetime
from tabulate import tabulate
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import pandas as pd
from dotenv import load_dotenv
load_dotenv()
import os

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "sslmode": os.getenv("DB_SSLMODE", "require"),
}

WEATHERBIT_KEY = os.getenv("WEATHERBIT_API_KEY")
WEATHERBIT_CITY = os.getenv("WEATHERBIT_CITY", "Maia,PT")
API_URL = f"https://api.weatherbit.io/v2.0/current?city={WEATHERBIT_CITY}&key={WEATHERBIT_KEY}"


DEFAULT_EMAIL_FROM = "estagio.pipeline@example.com"
DEFAULT_EMAIL_TO = ["pedro.pimenta@cm-maia.pt", "gustavo.sa.martins@gmail.com"]

DEFAULT_PIPELINE_NAME = "GM-IPMA-METEO"


# ====== CONTEXTO (context aware) ======

def get_context():
    user = os.getenv("PIPELINE_USER", "gustavo")
    env = os.getenv("PIPELINE_ENV", "local")

    db_config = {
        "dbname": os.getenv("DB_MAIN_NAME", DEFAULT_DB_CONFIG["dbname"]),
        "user": os.getenv("DB_MAIN_USER", DEFAULT_DB_CONFIG["user"]),
        "password": os.getenv("DB_MAIN_PASSWORD", DEFAULT_DB_CONFIG["password"]),
        "host": os.getenv("DB_MAIN_HOST", DEFAULT_DB_CONFIG["host"]),
        "port": int(os.getenv("DB_MAIN_PORT", DEFAULT_DB_CONFIG["port"])),
        "sslmode": os.getenv("DB_MAIN_SSLMODE", DEFAULT_DB_CONFIG["sslmode"]),
    }

    # Nome da pipeline
    pipeline_name = os.getenv("PIPELINE_NAME", DEFAULT_PIPELINE_NAME)

    # API URL para IPMA
    api_url = os.getenv("PIPELINE_API_URL_IPMA", DEFAULT_API_URL)

    # Emails
    email_from = os.getenv("PIPELINE_EMAIL_FROM", DEFAULT_EMAIL_FROM)
    email_to_raw = os.getenv(
        "PIPELINE_EMAIL_TO",
        ",".join(DEFAULT_EMAIL_TO)
    )
    email_to = [e.strip() for e in email_to_raw.split(",") if e.strip()]

    # Endpoint / dashboard onde se podem ver os dados (para pôr no email)
    dashboard_url = os.getenv("PIPELINE_DASHBOARD_URL_METEO", "")

    return {
        "user": user,
        "env": env,
        "db_config": db_config,
        "pipeline_name": pipeline_name,
        "api_url": api_url,
        "email_from": email_from,
        "email_to": email_to,
        "dashboard_url": dashboard_url,
    }


# ====== 1. REQUEST / RECEIVE / PARSE ======

def request_data(api_url):
    start = time.time()
    response = requests.get(api_url, timeout=10)
    response.raise_for_status()
    elapsed = round(time.time() - start, 2)
    return response.json(), elapsed


def receive_data(json_data):
    start = time.time()
    if not json_data:
        raise ValueError("JSON vazio ou inválido")
    elapsed = round(time.time() - start, 2)
    return json_data, elapsed


def parse_data(json_data):
    start = time.time()
    parsed = []

    for timestamp, stations in json_data.items():
        if not isinstance(stations, dict):
            continue

        for station_id, values in stations.items():
            if not isinstance(values, dict):
                continue

            parsed.append({
                "fonte": "IPMA",
                "data": timestamp,
                "temp": values.get("temperatura"),
                "humidade": values.get("humidade"),
                "vento": values.get("intensidadeVento"),
                "vento_km": values.get("intensidadeVentoKM"),
                "pressao": values.get("pressao"),
                "precipitacao": values.get("precAcumulada"),
                "radiacao": values.get("radiacao"),
                "id_estacao": station_id,
                "id_direcc_vento": values.get("idDireccVento"),
                "lugar": None,
                "lat": None,
                "lon": None,
            })

    elapsed = round(time.time() - start, 2)
    return parsed, elapsed


# ====== 2. LIGAÇÃO BD (ainda com modo offline) ======

def connect_db(ctx):
    start = time.time()
    db_conf = ctx["db_config"]

    # Se o host ainda é "Nao tenho acesso", assumimos modo offline
    if not db_conf.get("host") or db_conf["host"].lower().startswith("nao"):
        print("Ligação offline simulada — base de dados não utilizada.")
        conn = None
    else:
        conn = psycopg2.connect(**db_conf)
        print("Ligação à base de dados estabelecida.")

    elapsed = round(time.time() - start, 2)
    return conn, elapsed


def prepare_query(parsed_data):
    start = time.time()
    query = """
        INSERT INTO meteo (
            fonte, data, temp, humidade, vento, pressao, precipitacao,
            lugar, lat, lon, regdata
        )
        VALUES (
            %(fonte)s,
            %(data)s,
            %(temp)s,
            %(humidade)s,
            %(vento)s,
            %(pressao)s,
            %(precipitacao)s,
            %(lugar)s,
            %(lat)s,
            %(lon)s,
            NOW()
        )
    """
    elapsed = round(time.time() - start, 2)
    return query, elapsed


def execute_query(conn, query, parsed_data, ctx):
    """
    Mantém o comportamento original:
      - se conn for None → guarda em CSV offline
      - se conn não for None → (podemos mais tarde passar a inserir na BD)
    """
    start = time.time()

    if conn is None:
        # Modo offline 
        df = pd.DataFrame(parsed_data)
        os.makedirs("offline_output", exist_ok=True)
        file_path = f"offline_output/meteo_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(file_path, index=False)
        print(f"Dados guardados offline em: {file_path}")
    else:
        
        with conn.cursor() as cur:
            cur.executemany(query, parsed_data)
        conn.commit()
        print(f"Dados inseridos na BD (pipeline {ctx['pipeline_name']}).")

    elapsed = round(time.time() - start, 2)
    return elapsed


def close_connection(conn):
    start = time.time()
    if conn is not None:
        conn.close()
        print("Ligação à base de dados fechada.")
    else:
        print("Fecho de ligação offline — nada a fechar.")
    elapsed = round(time.time() - start, 2)
    return elapsed


# ====== 3. EMAIL ======

def send_summary_email(summary_rows, ctx):
    html_table = tabulate(summary_rows, headers=["Etapa", "Status", "Watch Time (s)"], tablefmt="html")

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"Pipeline {ctx['pipeline_name']} - Relatório de Execução ({ctx['env']})"
    msg["From"] = ctx["email_from"]
    msg["To"] = ", ".join(ctx["email_to"])

    extra_info = f"<p>Pipeline: <b>{ctx['pipeline_name']}</b> | Ambiente: <b>{ctx['env']}</b> | Utilizador lógico: <b>{ctx['user']}</b></p>"

    # endpoint / dashboard, se existir
    if ctx["dashboard_url"]:
        extra_info += f"""
        <p>Pode verificar os dados em:
           <a href="{ctx['dashboard_url']}">{ctx['dashboard_url']}</a>
        </p>
        """

    body = f"""
    <html>
    <body>
        <p><b>Resumo da execução da pipeline de meteorologia:</b></p>
        {extra_info}
        {html_table}
        <br>
        <p>Data/hora de execução: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    </body>
    </html>
    """
    msg.attach(MIMEText(body, "html"))

    try:
        with smtplib.SMTP("localhost") as server:
            server.send_message(msg)
        print("Email enviado com sucesso.")
    except Exception as e:
        print(f"Falha ao enviar email: {e}")


# ====== 4. PIPELINES ======

def pipeline_meteo_normal(ctx):
    summary = []
    conn = None

    try:
        # Request
        data, t1 = request_data(ctx["api_url"])
        summary.append(["Request API", "OK", t1])

        # Recepção
        received, t2 = receive_data(data)
        summary.append(["Recepção", "OK", t2])

        # Parsing
        parsed, t3 = parse_data(received)
        summary.append(["Parsing", "OK", t3])

        # Ligação BD
        conn, t4 = connect_db(ctx)
        summary.append(["Ligação BD", "OK", t4])

        # Preparação Query
        query, t5 = prepare_query(parsed)
        summary.append(["Preparação Query", "OK", t5])

        # Execução Query (offline CSV ou BD, consoante o contexto)
        t6 = execute_query(conn, query, parsed, ctx)
        summary.append(["Execução Query", "OK", t6])

        # Fecho BD
        t7 = close_connection(conn)
        summary.append(["Fecho BD", "OK", t7])

    except Exception as e:
        summary.append(["Erro", f"FAIL ({e})", 0])
    finally:
        send_summary_email(summary, ctx)
        print(tabulate(summary, headers=["Etapa", "Status", "Watch Time (s)"]))


def pipeline_meteo_heavy(ctx):
    summary = []
    conn = None

    try:
        # Ligação BD primeiro
        conn, t1 = connect_db(ctx)
        summary.append(["Ligação BD", "OK", t1])

        # Request
        data, t2 = request_data(ctx["api_url"])
        summary.append(["Request API", "OK", t2])

        # Recepção
        received, t3 = receive_data(data)
        summary.append(["Recepção", "OK", t3])

        # Parsing
        parsed, t4 = parse_data(received)
        summary.append(["Parsing", "OK", t4])

        # Preparação Query
        query, t5 = prepare_query(parsed)
        summary.append(["Preparação Query", "OK", t5])

        # Execução Query
        t6 = execute_query(conn, query, parsed, ctx)
        summary.append(["Execução Query", "OK", t6])

    except Exception as e:
        try:
            if conn:
                close_connection(conn)
        except Exception:
            pass
        summary.append(["Erro", f"FAIL ({e})", 0])
    finally:
        if conn:
            t7 = close_connection(conn)
            summary.append(["Fecho BD", "OK", t7])
        send_summary_email(summary, ctx)
        print(tabulate(summary, headers=["Etapa", "Status", "Watch Time (s)"]))


if __name__ == "__main__":
    ctx = get_context()
    print(f"Iniciando pipeline meteorológica '{ctx['pipeline_name']}' (env={ctx['env']}, user={ctx['user']})...")
    pipeline_meteo_normal(ctx)
