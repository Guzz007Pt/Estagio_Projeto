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

# ---------------- core env keys ----------------
DEFAULT_PIPELINE_NAME = os.getenv("PIPELINE_NAME", "GM-IPMA")
DEFAULT_EMAIL_FROM = os.getenv("PIPELINE_EMAIL_FROM", "estagio.pipeline@example.com")
DEFAULT_EMAIL_TO = [e.strip() for e in os.getenv(
    "PIPELINE_EMAIL_TO",
    "pedro.pimenta@cm-maia.pt,gustavo.sa.martins@gmail.com"
).split(",") if e.strip()]

DEFAULT_DB_TARGETS_FILE = os.getenv("PIPELINE_DB_TARGETS_FILE", "db_targets.json")

# Dedupe mode:
# - "timestamp": dedupe by (fonte, data timestamp, lugar)
# - "date": dedupe by (fonte, day) (legacy)
# - "none": always insert
DEDUP_MODE = os.getenv("PIPELINE_DEDUP_MODE", "timestamp").strip().lower()

# ---------------- IPMA endpoints ----------------
IPMA_OBS_URL = "https://api.ipma.pt/open-data/observation/meteorology/stations/observations.json"
IPMA_STATIONS_URL = "https://api.ipma.pt/open-data/observation/meteorology/stations/stations.json"

# Allow override(PIPELINE_API_URL)
DEFAULT_API_URL = os.getenv("PIPELINE_API_URL", IPMA_OBS_URL)

# Optional: restrict to certain stations (e.g. only Maia)
# Example: IPMA_STATION_IDS=1234,5678
IPMA_STATION_IDS = [s.strip() for s in os.getenv("IPMA_STATION_IDS", "").split(",") if s.strip()]


IPMA_FETCH_STATIONS_META = os.getenv("IPMA_FETCH_STATIONS_META", "1").strip() == "1"

def _parse_station_names_env(s: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for part in (s or "").split(","):
        part = part.strip()
        if not part or ":" not in part:
            continue
        sid, name = part.split(":", 1)
        sid = sid.strip()
        name = name.strip()
        if sid and name:
            out[sid] = name
    return out




IPMA_STATION_NAME_MAP = _parse_station_names_env(os.getenv("IPMA_STATION_NAMES", ""))
_IPMA_STATION_MAP = None  # cache

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
        return os.getenv("PIPELINE_PCP_HEADER") or f"Task(s) of {self.ctx['pipeline_name']} ({self.ctx['env']})."

    def html_table(self) -> str:
        return tabulate(
            self.pcp_rows_total(),
            headers=[self._header0(), "watch time (secs)", "proc time (secs)"],
            tablefmt="html",
        )


# ---------------- API fetch / parse (IPMA-specific) ----------------
def request_data(api_url: str) -> dict:
    # IPMA observations.json
    resp = requests.get(api_url, timeout=30)
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


def _ts_utc_naive(value) -> datetime:
    dt = value if isinstance(value, datetime) else _parse_ts_any(value)
    dt = dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    dt = dt.replace(microsecond=0)
    return dt.replace(tzinfo=None)  # naive UTC


def _get_ipma_station_map() -> dict[str, dict]:
    """
    Returns mapping station_id -> {"lat": float|None, "lon": float|None, "name": str|None}
    Uses IPMA stations.json (GeoJSON).
    """
    global _IPMA_STATION_MAP
    if _IPMA_STATION_MAP is not None:
        return _IPMA_STATION_MAP

    _IPMA_STATION_MAP = {}
    if not IPMA_FETCH_STATIONS_META:
        return _IPMA_STATION_MAP

    try:
        r = requests.get(IPMA_STATIONS_URL, timeout=30)
        r.raise_for_status()
        data = r.json()
        features = data.get("features", []) if isinstance(data, dict) else []
        for feat in features:
            props = feat.get("properties", {}) if isinstance(feat, dict) else {}
            geom = feat.get("geometry", {}) if isinstance(feat, dict) else {}
            coords = geom.get("coordinates") if isinstance(geom, dict) else None

            sid = props.get("idEstacao") or props.get("id") or props.get("stationId")
            if sid is None:
                continue
            sid = str(sid)

            lon = lat = None
            if isinstance(coords, (list, tuple)) and len(coords) >= 2:
                # GeoJSON typically [lon, lat]
                lon, lat = coords[0], coords[1]

            _IPMA_STATION_MAP[sid] = {
                "lat": lat,
                "lon": lon,
                "name": props.get("localEstacao") or props.get("nome") or props.get("name")
            }
    except Exception:
        # metadata is optional; silently continue without it
        _IPMA_STATION_MAP = {}

    return _IPMA_STATION_MAP


def parse_data(json_data: dict) -> list[dict]:
    """
    Produz as mesmas rows q a pipeline principal:
      fonte, data(datetime), temp, humidade, vento, pressao, precipitacao, lugar, lat, lon
    IPMA: lugar = id + name string
    filtro para n dar overflow: IPMA_STATION_IDS
    Optional fill: lat/lon via stations.json
    """
    station_map = _get_ipma_station_map()

    parsed: list[dict] = []
    if not isinstance(json_data, dict):
        return parsed

    for timestamp, stations in json_data.items():
        if not isinstance(stations, dict):
            continue
        ts_dt = _parse_ts_any(timestamp)

        for station_id, values in stations.items():
            if not isinstance(values, dict):
                continue

            sid = str(station_id)
            if IPMA_STATION_IDS and sid not in IPMA_STATION_IDS:
                continue

            meta = station_map.get(sid, {})

            # prioridade: .env mapping primeiro, depois stations.json mapping, senao fallback ao sid
            name_env = (IPMA_STATION_NAME_MAP.get(sid) or "").strip()
            name_meta = (meta.get("name") or "").strip()
            station_name = name_env or name_meta

            lugar = sid
            if station_name:
                lugar = f"{sid} - {station_name}"

            parsed.append({
                "fonte": "IPMA",
                "data": ts_dt,
                "temp": values.get("temperatura"),
                "humidade": values.get("humidade"),
                "vento": values.get("intensidadeVento"),
                "pressao": values.get("pressao"),
                "precipitacao": values.get("precAcumulada"),
                "lugar": lugar,          # <-- changed
                "lat": meta.get("lat"),
                "lon": meta.get("lon"),
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


# ---------------- DEDUPE + INSERT (same as main.py) ----------------
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
    out = []
    for r in rows:
        rr = dict(r)
        rr["data"] = _ts_utc_naive(rr.get("data"))
        out.append(tuple(rr.get(k) for k in COLUMNS))
    return out


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


def mongo_exists_day(handle: dict, fonte: str, day: date) -> bool:
    col = handle["col"]
    day_start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    day_end = day_start + timedelta(days=1)
    q = {"fonte": fonte, "data": {"$gte": day_start, "$lt": day_end}}
    return col.find_one(q, {"_id": 1}) is not None


def sql_existing_lugares_ts(handle: dict, fonte: str, ts: datetime) -> set[str]:
    table = handle["table"]
    conn = handle["conn"]
    ts = _ts_utc_naive(ts)

    sql = f"SELECT lugar FROM {table} WHERE fonte=%s AND data=%s"
    cur = conn.cursor()
    try:
        cur.execute(sql, (fonte, ts))
        rows = cur.fetchall() or []
        return {str(r[0]) for r in rows if r and r[0] is not None}
    finally:
        cur.close()


def mongo_existing_lugares_ts(handle: dict, fonte: str, ts: datetime) -> set[str]:
    col = handle["col"]
    ts = _ts_utc_naive(ts)
    q = {"fonte": fonte, "data": ts}
    docs = col.find(q, {"lugar": 1, "_id": 0})
    return {str(d.get("lugar")) for d in docs if d.get("lugar") is not None}


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


def mongo_insert_many(handle: dict, rows: list[dict]) -> None:
    col = handle["col"]
    now = datetime.now(timezone.utc)  # naive UTC
    docs = []
    for r in rows:
        d = dict(r)
        d["data"] = _ts_utc_naive(d.get("data"))
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


def build_batches_by_fonte_ts(rows: list[dict]) -> dict[tuple[str, datetime], list[dict]]:
    batches: dict[tuple[str, datetime], list[dict]] = {}
    for r in rows:
        fonte = r.get("fonte") or "UNKNOWN"
        ts = _ts_utc_naive(r.get("data"))
        batches.setdefault((fonte, ts), []).append(r)
    return batches


# ---------------- Email (same as main.py) ----------------
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

        if DEDUP_MODE == "date":
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

                    if exists:
                        skipped += len(batch_rows)
                        mon.mark(f"... `{day}` skipped for {fonte} @ {target_name} (dedup date)")
                        continue

                    if h["type"] == "mongodb":
                        mongo_insert_many(h, batch_rows)
                    else:
                        sql_insert_many(h, batch_rows)

                    inserted += len(batch_rows)
                    mon.mark(f"... `{day}` inserted {len(batch_rows)} row(s) for {fonte} @ {target_name}")

                mon.mark(f"Write summary @ {target_name}: ins={inserted}, skip={skipped}")

        elif DEDUP_MODE in ("timestamp", "ts"):
            batches = build_batches_by_fonte_ts(rows)
            mon.mark(f"Batching por fonte+timestamp ({len(batches)} batches)")

            for target_name, h in handles.items():
                inserted = 0
                skipped = 0

                for (fonte, ts), batch_rows in batches.items():
                    for r in batch_rows:
                        r["data"] = _ts_utc_naive(r.get("data"))

                    if h["type"] == "mongodb":
                        existing_lugares = mongo_existing_lugares_ts(h, fonte, ts)
                    else:
                        existing_lugares = sql_existing_lugares_ts(h, fonte, ts)

                    to_insert = [r for r in batch_rows if str(r.get("lugar")) not in existing_lugares]
                    skipped += (len(batch_rows) - len(to_insert))

                    if not to_insert:
                        mon.mark(f"... `{ts}` skipped for {fonte} @ {target_name} (dedup timestamp)")
                        continue

                    if h["type"] == "mongodb":
                        mongo_insert_many(h, to_insert)
                    else:
                        sql_insert_many(h, to_insert)

                    inserted += len(to_insert)
                    mon.mark(f"... `{ts}` inserted {len(to_insert)} row(s) for {fonte} @ {target_name}")

                mon.mark(f"Write summary @ {target_name}: ins={inserted}, skip={skipped}")

        else:
            batches = build_batches_by_fonte_ts(rows)
            mon.mark(f"Batching por fonte+timestamp ({len(batches)} batches)")

            for target_name, h in handles.items():
                inserted = 0
                for (fonte, ts), batch_rows in batches.items():
                    for r in batch_rows:
                        r["data"] = _ts_utc_naive(r.get("data"))

                    if h["type"] == "mongodb":
                        mongo_insert_many(h, batch_rows)
                    else:
                        sql_insert_many(h, batch_rows)

                    inserted += len(batch_rows)
                    mon.mark(f"... `{ts}` inserted {len(batch_rows)} row(s) for {fonte} @ {target_name}")

                mon.mark(f"Write summary @ {target_name}: ins={inserted}, skip=0")

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
