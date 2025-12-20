import os
import json
import requests
import psycopg2
from datetime import datetime, timezone
from tabulate import tabulate
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# ====== clts_pcp (monitorização) ======
try:
    import clts_pcp as clts  # pip install clts_pcp
    HAS_CLTS = True
except Exception:
    HAS_CLTS = False


# ====== Defaults  ======
DEFAULT_EMAIL_FROM = "estagio.pipeline@example.com"
DEFAULT_EMAIL_TO = ["pedro.pimenta@cm-maia.pt", "gustavo.sa.martins@gmail.com"]
DEFAULT_PIPELINE_NAME = "GM-METEO"

DEFAULT_DB_TARGETS_FILE = "db_targets.json"

# Fallback DB (se não houver targets file)
DEFAULT_DB_CONFIG = {
    "host": os.getenv("DB_HOST", ""),
    "port": int(os.getenv("DB_PORT", "5432")),
    "dbname": os.getenv("DB_NAME", ""),
    "user": os.getenv("DB_USER", ""),
    "password": os.getenv("DB_PASSWORD", ""),
    "sslmode": os.getenv("DB_SSLMODE", "require"),
}

# Weatherbit (se tiver key)
WEATHERBIT_KEY = os.getenv("WEATHERBIT_API_KEY", "")
WEATHERBIT_CITY = os.getenv("WEATHERBIT_CITY", "Maia,PT")
WEATHERBIT_URL = f"https://api.weatherbit.io/v2.0/current?city={WEATHERBIT_CITY}&key={WEATHERBIT_KEY}"

# IPMA (se quiser usar)
IPMA_URL = "https://api.ipma.pt/open-data/observation/meteorology/stations/observations.json"

# Default API URL (fallback)
DEFAULT_API_URL = WEATHERBIT_URL if WEATHERBIT_KEY else IPMA_URL


#  SQL (inserção + checker)
INSERT_SQL = """
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

# Checker por timestamp (recomendado para evitar duplicação por rerun)
CHECK_DUPLICATE_SQL = """
    SELECT 1
    FROM meteo
    WHERE fonte = %(fonte)s
      AND data = %(data)s
      AND (lugar IS NOT DISTINCT FROM %(lugar)s)
    LIMIT 1
"""

# Se quiseres “por dia”, troca por este e ajusta o dict de params se necessário:
# CHECK_DUPLICATE_SQL = """
#     SELECT 1
#     FROM meteo
#     WHERE fonte = %(fonte)s
#       AND CAST(data AS DATE) = CAST(%(data)s AS DATE)
#       AND (lugar IS NOT DISTINCT FROM %(lugar)s)
#     LIMIT 1
# """


# ====== CONTEXTO ======
def get_context():
    user = os.getenv("PIPELINE_USER", "gustavo")
    env = os.getenv("PIPELINE_ENV", "local")

    pipeline_name = os.getenv("PIPELINE_NAME", DEFAULT_PIPELINE_NAME)

    # Mantém compatibilidade: aceita PIPELINE_API_URL (novo) e PIPELINE_API_URL_IPMA (antigo)
    api_url = os.getenv("PIPELINE_API_URL", os.getenv("PIPELINE_API_URL_IPMA", DEFAULT_API_URL))

    email_from = os.getenv("PIPELINE_EMAIL_FROM", DEFAULT_EMAIL_FROM)
    email_to_raw = os.getenv("PIPELINE_EMAIL_TO", ",".join(DEFAULT_EMAIL_TO))
    email_to = [e.strip() for e in email_to_raw.split(",") if e.strip()]

    dashboard_url = os.getenv("PIPELINE_DASHBOARD_URL_METEO", "")

    db_targets_file = os.getenv("PIPELINE_DB_TARGETS_FILE", DEFAULT_DB_TARGETS_FILE)

    return {
        "user": user,
        "env": env,
        "pipeline_name": pipeline_name,
        "api_url": api_url,
        "email_from": email_from,
        "email_to": email_to,
        "dashboard_url": dashboard_url,
        "db_targets_file": db_targets_file,
    }


# ====== helpers (tempo / monitor) ======
def _clts_seconds(dt_obj):
    """
    clts.deltat() guarda watch/proc (no README mostra 2 colunas).
    Isto tenta apanhar "watch time" em segundos de forma tolerante.
    """
    if isinstance(dt_obj, (tuple, list)) and len(dt_obj) >= 1:
        return float(dt_obj[0])
    if isinstance(dt_obj, dict):
        for k in ("watch", "watch_time", "elapsed", "secs", "seconds"):
            if k in dt_obj:
                return float(dt_obj[k])
    try:
        return float(dt_obj)
    except Exception:
        return 0.0


class StepMonitor:
    def __init__(self, ctx):
        self.ctx = ctx
        self.summary = []

        self.use_clts = HAS_CLTS
        if self.use_clts:
            clts.setcontext(f"{ctx['pipeline_name']} ({ctx['env']})")
            self._ts_prev = clts.getts()

    def mark(self, step_name, status="OK"):
        if self.use_clts:
            dt = clts.deltat(self._ts_prev)
            clts.elapt[f"{step_name} ({status})."] = dt
            secs = round(_clts_seconds(dt), 2)
            self._ts_prev = clts.getts()
        else:
            # fallback simples se não tiver o pacote instalado
            secs = 0.0

        self.summary.append([step_name, status, secs])
        return secs

    def html_table(self):
        if self.use_clts:
            # README: listtimes() devolve HTML quando usado como retorno
            return clts.listtimes()
        return tabulate(self.summary, headers=["Etapa", "Status", "Watch Time (s)"], tablefmt="html")


# ====== 1) REQUEST / RECEIVE ======
def request_data(api_url):
    resp = requests.get(api_url, timeout=15)
    resp.raise_for_status()
    return resp.json()


def receive_data(json_data):
    if not json_data:
        raise ValueError("JSON vazio ou inválido")
    return json_data


# ====== 2) PARSE (compatível Weatherbit OU IPMA) ======
def _parse_ts_any(value):
    if isinstance(value, datetime):
        return value
    if isinstance(value, (int, float)):
        # epoch
        return datetime.fromtimestamp(value, tz=timezone.utc)

    s = str(value).strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"

    # tenta ISO
    try:
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        pass

    # tenta formatos comuns
    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except Exception:
            continue

    # último fallback: agora
    return datetime.now(timezone.utc)


def parse_data(json_data):
    # Weatherbit
    if isinstance(json_data, dict) and "data" in json_data and isinstance(json_data["data"], list) and json_data["data"]:
        obs = json_data["data"][0]

        # weatherbit costuma trazer ob_time 
        if "ob_time" in obs:
            dt = _parse_ts_any(obs["ob_time"])
        elif "ts" in obs:
            dt = _parse_ts_any(int(obs["ts"]))
        else:
            dt = datetime.now(timezone.utc)

        return [{
            "fonte": "WEATHERBIT",
            "data": dt,
            "temp": obs.get("temp"),
            "humidade": obs.get("rh"),
            "vento": obs.get("wind_spd"),
            "pressao": obs.get("pres"),
            "precipitacao": obs.get("precip"),
            "lugar": obs.get("city_name") or WEATHERBIT_CITY,
            "lat": obs.get("lat"),
            "lon": obs.get("lon"),
        }]

    # IPMA
    parsed = []
    if isinstance(json_data, dict):
        for timestamp, stations in json_data.items():
            if not isinstance(stations, dict):
                continue

            ts_dt = _parse_ts_any(timestamp)

            for station_id, values in stations.items():
                if not isinstance(values, dict):
                    continue

                parsed.append({
                    "fonte": "IPMA",
                    "data": ts_dt,
                    "temp": values.get("temperatura"),
                    "humidade": values.get("humidade"),
                    "vento": values.get("intensidadeVento"),
                    "pressao": values.get("pressao"),
                    "precipitacao": values.get("precAcumulada"),
                    # aqui uso lugar=station_id para conseguir deduplicar por estação
                    "lugar": str(station_id),
                    "lat": None,
                    "lon": None,
                })

    return parsed


# ====== 3) DB targets (ficheiro lista) ======
def load_db_targets(ctx):
    path = ctx["db_targets_file"]

    if path and os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)

        targets = raw["targets"] if isinstance(raw, dict) and "targets" in raw else raw
        if not isinstance(targets, list):
            raise ValueError("db_targets.json inválido: esperado uma lista ou {'targets':[...]}")

        # normaliza
        norm = []
        for i, t in enumerate(targets, start=1):
            if not isinstance(t, dict):
                continue
            name = t.get("name") or f"db{i}"
            norm.append({
                "name": name,
                "host": t.get("host", ""),
                "port": int(t.get("port", 5432)),
                "dbname": t.get("dbname", ""),
                "user": t.get("user", ""),
                "password": t.get("password", ""),
                "sslmode": t.get("sslmode", "require"),
            })
        return norm

    # fallback: 1 target via env DB_HOST/DB_PORT/...
    fb = DEFAULT_DB_CONFIG.copy()
    fb["name"] = "default_env"
    return [fb]


def connect_all_dbs(targets):
    conns = {}
    errors = {}

    for t in targets:
        name = t.get("name", "db")
        host = (t.get("host") or "").strip()

        # mantém o teu “modo offline” se host vazio ou "Nao..."
        if not host or host.lower().startswith("nao"):
            errors[name] = "offline/no-host"
            continue

        try:
            conn = psycopg2.connect(
                host=t["host"],
                port=int(t["port"]),
                dbname=t["dbname"],
                user=t["user"],
                password=t["password"],
                sslmode=t.get("sslmode", "require"),
            )
            conns[name] = conn
        except Exception as e:
            errors[name] = str(e)

    return conns, errors


def close_all(conns):
    for _, c in conns.items():
        try:
            c.close()
        except Exception:
            pass


# ====== 4) checker + insert ======
def is_duplicate(conn, row):
    params = {
        "fonte": row["fonte"],
        "data": row["data"],
        "lugar": row.get("lugar"),
    }
    with conn.cursor() as cur:
        cur.execute(CHECK_DUPLICATE_SQL, params)
        return cur.fetchone() is not None


def insert_rows(conn, rows):
    with conn.cursor() as cur:
        cur.executemany(INSERT_SQL, rows)
    conn.commit()


def offline_dump(rows):
    df = pd.DataFrame(rows)
    os.makedirs("offline_output", exist_ok=True)
    file_path = f"offline_output/meteo_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df.to_csv(file_path, index=False)
    return file_path


# ====== 5) Email ======
def send_summary_email(ctx, monitor, extra_lines=None):
    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"Pipeline {ctx['pipeline_name']} - Relatório de Execução ({ctx['env']})"
    msg["From"] = ctx["email_from"]
    msg["To"] = ", ".join(ctx["email_to"])

    extra_info = (
        f"<p>Pipeline: <b>{ctx['pipeline_name']}</b> | Ambiente: <b>{ctx['env']}</b> "
        f"| Utilizador lógico: <b>{ctx['user']}</b></p>"
    )

    if ctx["dashboard_url"]:
        extra_info += (
            f'<p>Pode verificar os dados em: '
            f'<a href="{ctx["dashboard_url"]}">{ctx["dashboard_url"]}</a></p>'
        )

    if extra_lines:
        extra_info += "<br>" + "<br>".join(f"<p>{line}</p>" for line in extra_lines)

    html_table = monitor.html_table()

    body = f"""
    <html>
    <body>
        <p><b>Resumo da execução da pipeline:</b></p>
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


# ====== PIPELINE (normal) ======
def pipeline_meteo(ctx):
    mon = StepMonitor(ctx)
    extra_lines = []

    conns = {}
    conn_errors = {}

    try:
        # Request
        data = request_data(ctx["api_url"])
        mon.mark("Request API", "OK")

        # Receive
        received = receive_data(data)
        mon.mark("Recepção", "OK")

        # Parse
        parsed = parse_data(received)
        if not parsed:
            raise ValueError("Parsing devolveu 0 linhas (verifica API/parse).")
        mon.mark(f"Parsing ({len(parsed)} linhas)", "OK")

        # Load targets
        targets = load_db_targets(ctx)
        mon.mark(f"Load DB targets ({len(targets)})", "OK")

        # Connect all
        conns, conn_errors = connect_all_dbs(targets)
        extra_lines.append(f"DBs ligadas: {', '.join(conns.keys()) if conns else '(nenhuma)'}")
        if conn_errors:
            extra_lines.append("DBs com erro/offline: " + "; ".join([f"{k}={v}" for k, v in conn_errors.items()]))

        mon.mark("Ligação BD(s)", "OK" if conns else "WARN")

        # Se não há conns → offline dump
        if not conns:
            fp = offline_dump(parsed)
            mon.mark("Persistência offline (CSV)", "OK")
            extra_lines.append(f"CSV offline: {fp}")
            return mon, extra_lines

        # Insert por DB com checker
        for dbname, conn in conns.items():
            inserted = 0
            skipped = 0

            for row in parsed:
                if is_duplicate(conn, row):
                    skipped += 1
                else:
                    insert_rows(conn, [row])
                    inserted += 1

            mon.mark(f"DB insert [{dbname}] (ins={inserted}, skip={skipped})", "OK")

    except Exception as e:
        mon.mark("Erro", f"FAIL ({e})")
        extra_lines.append(f"Erro: {e}")

    finally:
        try:
            close_all(conns)
            mon.mark("Fecho BD(s)", "OK")
        except Exception:
            pass

        send_summary_email(ctx, mon, extra_lines=extra_lines)
        print(tabulate(mon.summary, headers=["Etapa", "Status", "Watch Time (s)"]))

    return mon, extra_lines


if __name__ == "__main__":
    ctx = get_context()
    print(f"Iniciando pipeline '{ctx['pipeline_name']}' (env={ctx['env']}, user={ctx['user']})...")
    pipeline_meteo(ctx)
