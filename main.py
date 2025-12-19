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

    for item in json_data.get("data", []):
        parsed.append({
            "fonte": "Weatherbit",
            "data": item.get("ob_time"),
            "cidade": item.get("city_name"),
            "pais": item.get("country_code"),
            "temp": item.get("temp"),
            "sensacao_termica": item.get("app_temp"),
            "humidade": item.get("rh"),
            "vento": item.get("wind_spd"),
            "vento_dir": item.get("wind_dir"),
            "vento_desc": item.get("wind_cdir_full"),
            "pressao": item.get("pres"),
            "precipitacao": item.get("precip"),
            "uv": item.get("uv"),
            "radiacao_solar": item.get("solar_rad"),
            "nuvens": item.get("clouds"),
            "condicao": item.get("weather", {}).get("description"),
            "estacao": item.get("station"),
            "nascer_sol": item.get("sunrise"),
            "por_sol": item.get("sunset"),
            "lat": item.get("lat"),
            "lon": item.get("lon")
        })

    elapsed = round(time.time() - start, 2)
    return parsed, elapsed

def connect_db():
    start = time.time()
    print("Ligação offline simulada — base de dados não utilizada.")
    elapsed = round(time.time() - start, 2)
    return None, elapsed

def prepare_query(parsed_data):
    start = time.time()
    query = """
        INSERT INTO meteo (fonte, data, temp, humidade, vento, pressao, precipitacao, lugar, lat, lon, regdata)
        VALUES (%(fonte)s, %(data)s, %(temp)s, %(humidade)s, %(vento)s, %(pressao)s, %(precipitacao)s, %(lugar)s, %(lat)s, %(lon)s, NOW())
    """
    elapsed = round(time.time() - start, 2)
    return query, elapsed

def execute_query(_, __, parsed_data):
    start = time.time()
    df = pd.DataFrame(parsed_data)
    os.makedirs("offline_output", exist_ok=True)
    file_path = f"offline_output/meteo_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df.to_csv(file_path, index=False)
    elapsed = round(time.time() - start, 2)
    print(f"Dados guardados offline em: {file_path}")
    return elapsed

def close_connection(_):
    start = time.time()
    print("Fecho de ligação offline — nada a fechar.")
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
