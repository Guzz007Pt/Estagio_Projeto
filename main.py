import os
import json
import requests
import time
from datetime import datetime, timezone, timedelta, date
from urllib.parse import urlparse, unquote
from tabulate import tabulate
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import pandas as pd
from dotenv import load_dotenv

# ---------- optional monitoring (clts_pcp) ----------
try:
    import clts_pcp as clts  # pip install clts_pcp
    HAS_CLTS = True
except Exception:
    HAS_CLTS = False

# ---------- DB drivers ----------
try:
    import psycopg2  # pip install psycopg2-binary
    HAS_PG = True
except Exception:
    HAS_PG = False

try:
    import pymysql  # pip install pymysql
    HAS_PYMYSQL = True
except Exception:
    HAS_PYMYSQL = False

try:
    import mysql.connector  # pip install mysql-connector-python
    HAS_MYSQL_CONNECTOR = True
except Exception:
    HAS_MYSQL_CONNECTOR = False

try:
    from pymongo import MongoClient  # pip install pymongo
    HAS_MONGO = True
except Exception:
    HAS_MONGO = False


load_dotenv()

DEFAULT_PIPELINE_NAME = os.getenv("PIPELINE_NAME", "GM-METEO")
DEFAULT_EMAIL_FROM = os.getenv("PIPELINE_EMAIL_FROM", "estagio.pipeline@example.com")
DEFAULT_EMAIL_TO = [e.strip() for e in os.getenv(
    "PIPELINE_EMAIL_TO",
    "pedro.pimenta@cm-maia.pt,gustavo.sa.martins@gmail.com"
).split(",") if e.strip()]

DEFAULT_DB_TARGETS_FILE = os.getenv("PIPELINE_DB_TARGETS_FILE", "db_targets.json")

# Dedupe mode: "date" blocks re-inserting the same day for the same fonte
DEDUP_MODE = os.getenv("PIPELINE_DEDUP_MODE", "date").strip().lower()

# ---- API defaults: Weatherbit if key else IPMA ----
WEATHERBIT_KEY = os.getenv("WEATHERBIT_API_KEY", "")
WEATHERBIT_CITY = os.getenv("WEATHERBIT_CITY", "Maia,PT")
WEATHERBIT_URL = f"https://api.weatherbit.io/v2.0/current?city={WEATHERBIT_CITY}&key={WEATHERBIT_KEY}"
IPMA_URL = "https://api.ipma.pt/open-data/observation/meteorology/stations/observations.json"
DEFAULT_API_URL = os.getenv("PIPELINE_API_URL", WEATHERBIT_URL if WEATHERBIT_KEY else IPMA_URL)


def get_context() -> dict:
    return {
        "pipeline_name": DEFAULT_PIPELINE_NAME,
        "env": os.getenv("PIPELINE_ENV", "local"),
        "user": os.getenv("PIPELINE_USER", "gustavo"),
        "api_url": DEFAULT_API_URL,
        "email_from": DEFAULT_EMAIL_FROM,
        "email_to": DEFAULT_EMAIL_TO,
        "dashboard_url": os.getenv("PIPELINE_DASHBOARD_URL_METEO", ""),
        "db_targets_file": DEFAULT_DB_TARGETS_FILE,
    }


# ---------------- monitoring helper ----------------
def _clts_watch_proc(dt_obj) -> tuple[float, float]:
    # clts.deltat costuma devolver (watch, proc)
    if isinstance(dt_obj, (tuple, list)):
        w = float(dt_obj[0]) if len(dt_obj) > 0 else 0.0
        p = float(dt_obj[1]) if len(dt_obj) > 1 else 0.0
        return w, p
    if isinstance(dt_obj, dict):
        w = float(dt_obj.get("watch", dt_obj.get("watch_time", dt_obj.get("elapsed", 0.0))) or 0.0)
        p = float(dt_obj.get("proc", dt_obj.get("proc_time", 0.0)) or 0.0)
        return w, p
    try:
        return float(dt_obj), 0.0
    except Exception:
        return 0.0, 0.0


class StepMonitor:
    def __init__(self, ctx: dict):
        self.ctx = ctx
        self.summary: list[dict] = []  # {"step","status","watch","proc"}
        self.use_clts = HAS_CLTS and os.getenv("USE_CLTS_PCP", "0") == "1"

        self._w_prev = time.perf_counter()
        self._p_prev = time.process_time()

        if self.use_clts:
            clts.setcontext(f"{ctx['pipeline_name']} ({ctx['env']})")
            self._ts_prev = clts.getts()

    def mark(self, step: str, status: str = "", seconds_override: float | None = None) -> float:
        if seconds_override is not None:
            watch = float(seconds_override)
            proc = 0.0
        elif self.use_clts:
            dt = clts.deltat(self._ts_prev)
            watch, proc = _clts_watch_proc(dt)
            self._ts_prev = clts.getts()
        else:
            w_now = time.perf_counter()
            p_now = time.process_time()
            watch = w_now - self._w_prev
            proc = p_now - self._p_prev
            self._w_prev = w_now
            self._p_prev = p_now

        self.summary.append({"step": str(step), "status": str(status), "watch": float(watch), "proc": float(proc)})
        return float(watch)

    def pcp_rows_total(self) -> list[list]:
        w_total = 0.0
        p_total = 0.0
        rows: list[list] = []

        for r in self.summary:
            w_total += r["watch"]
            p_total += r["proc"]

            label = r["step"]
            if r["status"]:
                label = f"{label} ({r['status']})"

            rows.append([label, round(w_total, 2), round(p_total, 2)])

        rows.append(["Overall (before email):", round(w_total, 2), round(p_total, 2)])
        return rows

    def _header0(self) -> str:
        # opcional: deixa-te pôr um header exatamente como o PCP via env var
        # ex: PIPELINE_PCP_HEADER="Task(s) of TP16G2-PCP (176.223.60.34) | PCP | scripts | pcp_meteo_icao.py | -*."
        return os.getenv("PIPELINE_PCP_HEADER") or f"Task(s) of {self.ctx['pipeline_name']} ({self.ctx['env']})."

    def html_table(self) -> str:
        return tabulate(
            self.pcp_rows_total(),
            headers=[self._header0(), "watch time (secs)", "proc time (secs)"],
            tablefmt="html",
        )


# ---------------- API fetch / parse ----------------
def request_data(api_url: str) -> dict:
    resp = requests.get(api_url, timeout=20)
    resp.raise_for_status()
    return resp.json()


def receive_data(json_data: dict) -> dict:
    if not json_data:
        raise ValueError("JSON vazio ou inválido")
    return json_data


def _parse_ts_any(value) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc)

    s = str(value).strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"

    try:
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        pass

    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except Exception:
            continue

    return datetime.now(timezone.utc)


def parse_data(json_data: dict) -> list[dict]:
    """
    Canonical row format (works for every DB target):
      fonte, data(datetime), temp, humidade, vento, pressao, precipitacao, lugar, lat, lon
    """
    # Weatherbit
    if isinstance(json_data, dict) and "data" in json_data and isinstance(json_data["data"], list) and json_data["data"]:
        obs = json_data["data"][0]
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
    parsed: list[dict] = []
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
                    "lugar": str(station_id),
                    "lat": None,
                    "lon": None,
                })
    return parsed


# ---------------- db_targets loader ----------------
def _safe_ident(name: str) -> str:
    name = (name or "").strip()
    if not name:
        raise ValueError("Nome de tabela vazio.")
    if not all(c.isalnum() or c == "_" for c in name):
        raise ValueError(f"Nome de tabela inválido: {name!r}")
    return name


def _get_env_or_value(obj: dict, value_key: str, env_key_key: str) -> str:
    v = obj.get(value_key)
    if isinstance(v, str) and v.strip():
        return v.strip()
    env_name = obj.get(env_key_key)
    if isinstance(env_name, str) and env_name.strip():
        return os.getenv(env_name.strip(), "").strip()
    return ""


def load_db_targets(ctx: dict) -> list[dict]:
    path = ctx["db_targets_file"]
    if not path or not os.path.exists(path):
        raise ValueError(f"Ficheiro de targets não encontrado: {path!r}")

    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    targets = raw["targets"] if isinstance(raw, dict) and "targets" in raw else raw
    if not isinstance(targets, list):
        raise ValueError("db_targets.json inválido: esperado lista ou {'targets':[...]}")

    norm: list[dict] = []
    for i, t in enumerate(targets, start=1):
        if not isinstance(t, dict):
            continue

        ttype = (t.get("type") or "postgres").strip().lower()
        name = t.get("name") or f"db{i}"

        if ttype in ("postgres", "cockroachdb", "yugabyte", "supabase", "neon", "aiven_pg"):
            dsn = _get_env_or_value(t, "dsn", "dsn_env")
            table = _safe_ident(t.get("table", "meteo"))
            norm.append({"name": name, "type": "postgres", "dsn": dsn, "table": table})

        elif ttype in ("cratedb",):
            dsn = _get_env_or_value(t, "dsn", "dsn_env")
            table = _safe_ident(t.get("table", "meteo"))
            norm.append({"name": name, "type": "cratedb", "dsn": dsn, "table": table})

        elif ttype in ("mysql", "mariadb", "tidb", "tidbcloud"):
            dsn = _get_env_or_value(t, "dsn", "dsn_env")
            table = _safe_ident(t.get("table", "meteo"))
            norm.append({"name": name, "type": "mysql", "dsn": dsn, "table": table})

        elif ttype in ("mongodb", "mongo"):
            uri = _get_env_or_value(t, "uri", "uri_env")
            database = (t.get("database") or "meteo").strip()
            collection = (t.get("collection") or "meteo").strip()
            norm.append({"name": name, "type": "mongodb", "uri": uri, "database": database, "collection": collection})

        else:
            raise ValueError(f"Tipo de target desconhecido ({name}): {ttype!r}")

    return norm


# ---------------- DB connection helpers ----------------
def _parse_mysql_dsn(dsn: str) -> dict:
    p = urlparse(dsn)
    if p.scheme not in ("mysql", "mariadb"):
        raise ValueError(f"MySQL DSN inválido: {dsn!r}")

    user = unquote(p.username or "")
    password = unquote(p.password or "")
    host = p.hostname or ""
    port = p.port or 3306
    dbname = (p.path or "").lstrip("/")
    if not host or not dbname:
        raise ValueError("MySQL DSN precisa de host e dbname.")

    return {"host": host, "port": port, "user": user, "password": password, "database": dbname}


def connect_targets(targets: list[dict], mon: StepMonitor | None = None) -> tuple[dict, dict]:
    handles: dict[str, dict] = {}
    errors: dict[str, str] = {}

    for t in targets:
        name = t["name"]
        ttype = t["type"]

        if mon:
            mon.mark(f"Connecting to `{name}`")

        try:
            if ttype in ("postgres", "cratedb"):
                if not HAS_PG:
                    raise RuntimeError("psycopg2 não instalado (pip install psycopg2-binary).")
                dsn = (t.get("dsn") or "").strip()
                if not dsn:
                    raise RuntimeError("DSN vazio (dsn/dsn_env).")
                conn = psycopg2.connect(dsn)
                handles[name] = {"type": ttype, "conn": conn, "table": t["table"]}

            elif ttype == "mysql":
                dsn = (t.get("dsn") or "").strip()
                if not dsn:
                    raise RuntimeError("DSN vazio (dsn/dsn_env).")

                mysql_kwargs = _parse_mysql_dsn(dsn)

                if HAS_PYMYSQL:
                    conn = pymysql.connect(**mysql_kwargs, autocommit=False)
                    driver = "pymysql"
                elif HAS_MYSQL_CONNECTOR:
                    conn = mysql.connector.connect(**mysql_kwargs)
                    driver = "mysql-connector"
                else:
                    raise RuntimeError("Instala pymysql ou mysql-connector-python para targets MySQL.")

                handles[name] = {"type": "mysql", "conn": conn, "table": t["table"], "driver": driver}

            elif ttype == "mongodb":
                if not HAS_MONGO:
                    raise RuntimeError("pymongo não instalado (pip install pymongo).")
                uri = (t.get("uri") or "").strip()
                if not uri:
                    raise RuntimeError("MongoDB URI vazio (uri/uri_env).")
                client = MongoClient(uri)
                col = client[t["database"]][t["collection"]]
                handles[name] = {"type": "mongodb", "client": client, "col": col}

            else:
                raise RuntimeError(f"type não suportado: {ttype}")

            if mon:
                mon.mark(f"... connected to `{name}`")

        except Exception as e:
            errors[name] = str(e)
            if mon:
                mon.mark(f"... failed to connect to `{name}`: {e}", "FAIL")

    return handles, errors

def close_targets(handles: dict) -> None:
    for h in handles.values():
        try:
            if h["type"] in ("postgres", "cratedb", "mysql"):
                h["conn"].close()
            elif h["type"] == "mongodb":
                h["client"].close()
        except Exception:
            pass


# ---------------- DEDUPE + INSERT ----------------
COLUMNS = ["fonte", "data", "temp", "humidade", "vento", "pressao", "precipitacao", "lugar", "lat", "lon"]


def _row_day_utc(r: dict) -> date:
    dt = r["data"]
    dt = dt if isinstance(dt, datetime) else _parse_ts_any(dt)
    dt = dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    return dt.date()


def build_insert_sql(table: str) -> str:
    cols_sql = ", ".join(COLUMNS + ["regdata"])
    ph = ", ".join(["%s"] * len(COLUMNS) + ["CURRENT_TIMESTAMP"])
    return f"INSERT INTO {table} ({cols_sql}) VALUES ({ph})"


def rows_to_tuples(rows: list[dict]) -> list[tuple]:
    return [tuple(r.get(k) for k in COLUMNS) for r in rows]


def sql_exists_day(handle: dict, fonte: str, day: date) -> bool:
    table = handle["table"]
    conn = handle["conn"]
    ttype = handle["type"]

    if ttype == "mysql":
        sql = f"SELECT 1 FROM {table} WHERE fonte=%s AND DATE(data)=%s LIMIT 1"
        params = (fonte, day)
    else:
        sql = f"SELECT 1 FROM {table} WHERE fonte=%s AND CAST(data AS DATE)=%s LIMIT 1"
        params = (fonte, day)

    cur = conn.cursor()
    try:
        cur.execute(sql, params)
        return cur.fetchone() is not None
    finally:
        cur.close()


def sql_insert_many(handle: dict, rows: list[dict]) -> None:
    table = handle["table"]
    conn = handle["conn"]
    insert_sql = build_insert_sql(table)
    values = rows_to_tuples(rows)

    cur = conn.cursor()
    try:
        cur.executemany(insert_sql, values)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def mongo_exists_day(handle: dict, fonte: str, day: date) -> bool:
    col = handle["col"]
    day_start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    day_end = day_start + timedelta(days=1)
    q = {"fonte": fonte, "data": {"$gte": day_start, "$lt": day_end}}
    return col.find_one(q, {"_id": 1}) is not None


def mongo_insert_many(handle: dict, rows: list[dict]) -> None:
    col = handle["col"]
    now = datetime.now(timezone.utc)
    docs = []
    for r in rows:
        d = dict(r)
        d["regdata"] = now
        docs.append(d)
    if docs:
        col.insert_many(docs, ordered=False)


def offline_dump(rows: list[dict]) -> str:
    df = pd.DataFrame(rows)
    os.makedirs("offline_output", exist_ok=True)
    fp = f"offline_output/meteo_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df.to_csv(fp, index=False)
    return fp


def build_batches_by_fonte_day(rows: list[dict]) -> dict[tuple[str, date], list[dict]]:
    batches: dict[tuple[str, date], list[dict]] = {}
    for r in rows:
        fonte = r.get("fonte") or "UNKNOWN"
        day = _row_day_utc(r)
        batches.setdefault((fonte, day), []).append(r)
    return batches


# ---------------- Email ----------------
def send_summary_email(ctx: dict, mon: StepMonitor, extra_lines: list[str] | None = None) -> None:
    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"Pipeline {ctx['pipeline_name']} - Relatório de Execução ({ctx['env']})"
    msg["To"] = ", ".join(ctx["email_to"])

    extra_info = (
        f"<p>Pipeline: <b>{ctx['pipeline_name']}</b> | Ambiente: <b>{ctx['env']}</b> | "
        f"Utilizador lógico: <b>{ctx['user']}</b> | DEDUP_MODE: <b>{DEDUP_MODE}</b></p>"
    )

    if ctx.get("dashboard_url"):
        url = ctx["dashboard_url"]
        extra_info += f'<p>Pode verificar os dados em: <a href="{url}">{url}</a></p>'

    if extra_lines:
        extra_info += "<br>" + "<br>".join(f"<p>{line}</p>" for line in extra_lines)

    body = f"""
    <html>
    <body>
        <p><b>Resumo da execução da pipeline:</b></p>
        {extra_info}
        {mon.html_table()}
        <br>
        <p>Data/hora de execução: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    </body>
    </html>
    """
    msg.attach(MIMEText(body, "html"))

    try:
        smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com")
        smtp_port = int(os.getenv("SMTP_PORT", "587"))
        smtp_user = os.getenv("SMTP_USER")
        smtp_pass = os.getenv("SMTP_PASS")

        if not smtp_user or not smtp_pass:
            raise RuntimeError("Faltam SMTP_USER / SMTP_PASS no .env")


        email_from = os.getenv("PIPELINE_EMAIL_FROM") or smtp_user or ctx["email_from"]
        if "From" in msg:
            msg.replace_header("From", email_from)
        else:
            msg["From"] = email_from


        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(smtp_user, smtp_pass)
            server.send_message(msg)

        print("Email enviado com sucesso.")
    except Exception as e:
        print(f"Falha ao enviar email: {e}")


# ---------------- Pipeline ----------------
def pipeline_meteo(ctx: dict):
    mon = StepMonitor(ctx)
    extra_lines: list[str] = []
    handles: dict[str, dict] = {}
    errors: dict[str, str] = {}

    try:
        data = request_data(ctx["api_url"])
        mon.mark("Data request successful!")

        received = receive_data(data)
        mon.mark("Data reception successful!")

        rows = parse_data(received)
        if not rows:
            raise ValueError("Parsing devolveu 0 linhas (verifica a API / parse).")
        mon.mark(f"Parsing ({len(rows)} linhas)")

        targets = load_db_targets(ctx)
        mon.mark(f"Load DB targets ({len(targets)})")

        mon.mark("Starting database accesses:")
        handles, errors = connect_targets(targets, mon=mon)

        extra_lines.append(f"Targets ligados: {', '.join(handles.keys()) if handles else '(nenhum)'}")
        if errors:
            extra_lines.append("Targets com erro: " + "; ".join([f"{k}={v}" for k, v in errors.items()]))

        if not handles:
            fp = offline_dump(rows)
            mon.mark(f"Data saved to file `{fp}`")
            return mon, extra_lines

        batches = build_batches_by_fonte_day(rows)
        mon.mark(f"Batching por fonte+dia ({len(batches)} batches)")

        for target_name, h in handles.items():
            inserted = 0
            skipped = 0

            for (fonte, day), batch_rows in batches.items():
                if h["type"] == "mongodb":
                    exists = mongo_exists_day(h, fonte, day)
                else:
                    exists = sql_exists_day(h, fonte, day)

                if exists and DEDUP_MODE == "date":
                    skipped += len(batch_rows)
                    mon.mark(f"... `{day}` skipped for {fonte} @ {target_name} (dedup)")
                    continue

                if h["type"] == "mongodb":
                    mongo_insert_many(h, batch_rows)
                else:
                    sql_insert_many(h, batch_rows)

                inserted += len(batch_rows)
                mon.mark(f"... `{day}` inserted {len(batch_rows)} row(s) for {fonte} @ {target_name}")

            # linha resumo (opcional, mas útil)
            mon.mark(f"Write summary @ {target_name}: ins={inserted}, skip={skipped}")

    except Exception as e:
        mon.mark(f"Erro: {e}", "FAIL")
        extra_lines.append(f"Erro: {e}")

    finally:
        try:
            close_targets(handles)
            mon.mark("Fecho targets")
        except Exception:
            pass

        send_summary_email(ctx, mon, extra_lines=extra_lines)

        print(tabulate(
            mon.pcp_rows_total(),
            headers=[os.getenv("PIPELINE_PCP_HEADER") or f"Task(s) of {ctx['pipeline_name']} ({ctx['env']}).",
                     "watch time (secs)", "proc time (secs)"]
        ))

    return mon, extra_lines


if __name__ == "__main__":
    ctx = get_context()
    print(f"Iniciando pipeline '{ctx['pipeline_name']}' (env={ctx['env']}, user={ctx['user']})...")
    pipeline_meteo(ctx)
