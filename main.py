import requests
import psycopg2
import time
from datetime import datetime
from tabulate import tabulate
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
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

API_URL = "https://api.ipma.pt/open-data/observation/meteorology/stations/observations.json"

EMAIL_FROM = "estagio.pipeline@example.com"
EMAIL_TO = ["pedro.pimenta@cm-maia.pt", "gustavo.sa.martins@gmail.com"]

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
                "id_direcc_vento": values.get("idDireccVento")
            })

    elapsed = round(time.time() - start, 2)
    return parsed, elapsed

def connect_db():
    start = time.time()
    conn = psycopg2.connect(**DB_CONFIG)
    elapsed = round(time.time() - start, 2)
    return conn, elapsed

def prepare_query(parsed_data):
    start = time.time()
    query = """
        INSERT INTO meteo (fonte, data, temp, humidade, vento, pressao, precipitacao, lugar, lat, lon, regdata)
        VALUES (%(fonte)s, %(data)s, %(temp)s, %(humidade)s, %(vento)s, %(pressao)s, %(precipitacao)s, %(lugar)s, %(lat)s, %(lon)s, NOW())
    """
    elapsed = round(time.time() - start, 2)
    return query, elapsed

def execute_query(conn, query, parsed_data):
    start = time.time()
    with conn.cursor() as cur:
        cur.executemany(query, parsed_data)
        conn.commit()
    elapsed = round(time.time() - start, 2)
    return elapsed

def close_connection(conn):
    start = time.time()
    conn.close()
    elapsed = round(time.time() - start, 2)
    return elapsed

def send_summary_email(summary_rows):
    html_table = tabulate(summary_rows, headers=["Etapa", "Status", "Watch Time (s)"], tablefmt="html")

    msg = MIMEMultipart("alternative")
    msg["Subject"] = "Pipeline Meteo - Relatório de Execução"
    msg["From"] = EMAIL_FROM
    msg["To"] = ", ".join(EMAIL_TO)

    body = f"""
    <html>
    <body>
        <p><b>Resumo da execução da pipeline de meteorologia:</b></p>
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

def pipeline_meteo_normal(api_url):
    summary = []
    try:
        data, t1 = request_data(api_url)
        summary.append(["Request API", "OK", t1])
        received, t2 = receive_data(data)
        summary.append(["Recepção", "OK", t2])
        parsed, t3 = parse_data(received)
        summary.append(["Parsing", "OK", t3])
        conn, t4 = connect_db()
        summary.append(["Ligação BD", "OK", t4])
        query, t5 = prepare_query(parsed)
        summary.append(["Preparação Query", "OK", t5])
        t6 = execute_query(conn, query, parsed)
        summary.append(["Execução Query", "OK", t6])
        t7 = close_connection(conn)
        summary.append(["Fecho BD", "OK", t7])
    except Exception as e:
        summary.append(["Erro", f"FAIL ({e})", 0])
    finally:
        send_summary_email(summary)
        print(tabulate(summary, headers=["Etapa", "Status", "Watch Time (s)"]))

def pipeline_meteo_heavy(api_url):
    summary = []
    conn = None
    try:
        conn, t1 = connect_db()
        summary.append(["Ligação BD", "OK", t1])
        data, t2 = request_data(api_url)
        summary.append(["Request API", "OK", t2])
        received, t3 = receive_data(data)
        summary.append(["Recepção", "OK", t3])
        parsed, t4 = parse_data(received)
        summary.append(["Parsing", "OK", t4])
        query, t5 = prepare_query(parsed)
        summary.append(["Preparação Query", "OK", t5])
        t6 = execute_query(conn, query, parsed)
        summary.append(["Execução Query", "OK", t6])
        t7 = close_connection(conn)
        summary.append(["Fecho BD", "OK", t7])
    except Exception as e:
        try:
            if conn:
                close_connection(conn)
        except Exception:
            pass
        summary.append(["Erro", f"FAIL ({e})", 0])
    finally:
        send_summary_email(summary)
        print(tabulate(summary, headers=["Etapa", "Status", "Watch Time (s)"]))

if __name__ == "__main__":
    print("Iniciando pipeline meteorológica (normal)...")
    pipeline_meteo_normal(API_URL)
