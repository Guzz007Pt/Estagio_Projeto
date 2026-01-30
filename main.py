import os
import json
import time
import csv
import requests
import smtplib
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse, unquote
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Optional libs (only if installed)
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

try:
    from tabulate import tabulate  # pip install tabulate
    HAS_TABULATE = True
except Exception:
    HAS_TABULATE = False

try:
    from dotenv import load_dotenv  # pip install python-dotenv
    load_dotenv()
except Exception:
    pass


# =========================
# Config / Context
# =========================
PIPELINE_NAME = os.getenv("PIPELINE_NAME", "GM-METEO")
PIPELINE_ENV = os.getenv("PIPELINE_ENV", "local")
PIPELINE_USER = os.getenv("PIPELINE_USER", "gustavo")

EMAIL_FROM_DEFAULT = os.getenv("PIPELINE_EMAIL_FROM", "estagio.pipeline@example.com")
EMAIL_TO_DEFAULT = [
    e.strip() for e in os.getenv(
        "PIPELINE_EMAIL_TO",
        "pedro.pimenta@cm-maia.pt,gustavo.sa.martins@gmail.com"
    ).split(",")
    if e.strip()
]

DB_TARGETS_FILE = os.getenv("PIPELINE_DB_TARGETS_FILE", "db_targets.json")


WEATHERBIT_KEY = os.getenv("WEATHERBIT_API_KEY", "")
WEATHERBIT_CITY = os.getenv("WEATHERBIT_CITY", "Maia,PT")
WEATHERBIT_URL = f"https://api.weatherbit.io/v2.0/current?city={WEATHERBIT_CITY}&key={WEATHERBIT_KEY}"
IPMA_URL = "https://api.ipma.pt/open-data/observation/meteorology/stations/observations.json"
API_URL = os.getenv("PIPELINE_API_URL", WEATHERBIT_URL if WEATHERBIT_KEY else IPMA_URL)

DASHBOARD_URL = os.getenv("PIPELINE_DASHBOARD_URL_METEO", "")

ctx = {
    "pipeline_name": PIPELINE_NAME,
    "env": PIPELINE_ENV,
    "user": PIPELINE_USER,
    "api_url": API_URL,
    "email_from": EMAIL_FROM_DEFAULT,
    "email_to": EMAIL_TO_DEFAULT,
    "dashboard_url": DASHBOARD_URL,
    "db_targets_file": DB_TARGETS_FILE,
}

print(f"Iniciando pipeline '{ctx['pipeline_name']}' (env={ctx['env']}, user={ctx['user']})...")


# =========================
# Simple monitoring
# =========================
steps = []
w_prev = time.perf_counter()
p_prev = time.process_time()

extra_lines = []
handles = {}
errors = {}

try:
    # =========================
    # 1) Request data
    # =========================
    resp = requests.get(ctx["api_url"], timeout=20)
    resp.raise_for_status()
    json_data = resp.json()

    w_now = time.perf_counter()
    p_now = time.process_time()
    watch = w_now - w_prev
    proc = p_now - p_prev
    w_prev = w_now
    p_prev = p_now
    steps.append({'step': str("Data request successful!"), 'status': str(''), 'watch': float(watch), 'proc': float(proc)})

    if not json_data:
        raise ValueError("JSON vazio ou inválido")
    received = json_data

    w_now = time.perf_counter()
    p_now = time.process_time()
    watch = w_now - w_prev
    proc = p_now - p_prev
    w_prev = w_now
    p_prev = p_now
    steps.append({'step': str("Data reception successful!"), 'status': str(''), 'watch': float(watch), 'proc': float(proc)})

    # =========================
    # 2) Parse to canonical rows
    # =========================
    rows = []

    # Weatherbit shape: {"data":[{...}]}
    if isinstance(received, dict) and "data" in received and isinstance(received["data"], list) and received["data"]:
        obs = received["data"][0]

        # Parse timestamp
        dt = None
        if "ob_time" in obs:
            s = str(obs["ob_time"]).strip()
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            try:
                dt = datetime.fromisoformat(s)
            except Exception:
                dt = None

        if dt is None and "ts" in obs:
            try:
                dt = datetime.fromtimestamp(int(obs["ts"]), tz=timezone.utc)
            except Exception:
                dt = None

        if dt is None:
            dt = datetime.now(timezone.utc)

        rows.append({
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
        })

    # IPMA shape: { "<timestamp>": { "<station_id>": {...}, ... }, ... }
    else:
        if isinstance(received, dict):
            for timestamp, stations in received.items():
                if not isinstance(stations, dict):
                    continue

                # parse timestamp key
                ts_dt = None
                s = str(timestamp).strip()
                if s.endswith("Z"):
                    s = s[:-1] + "+00:00"

                try:
                    ts_dt = datetime.fromisoformat(s)
                except Exception:
                    ts_dt = None

                if ts_dt is None:
                    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
                        try:
                            ts_dt = datetime.strptime(str(timestamp).strip(), fmt).replace(tzinfo=timezone.utc)
                            break
                        except Exception:
                            continue

                if ts_dt is None:
                    ts_dt = datetime.now(timezone.utc)

                for station_id, values in stations.items():
                    if not isinstance(values, dict):
                        continue
                    rows.append({
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

    if not rows:
        raise ValueError("Parsing devolveu 0 linhas (verifica a API / parse).")

    w_now = time.perf_counter()
    p_now = time.process_time()
    watch = w_now - w_prev
    proc = p_now - p_prev
    w_prev = w_now
    p_prev = p_now
    steps.append({'step': str(f"Parsing ({len(rows)} linhas)"), 'status': str(''), 'watch': float(watch), 'proc': float(proc)})

    # =========================
    # 3) Normalize timestamps (UTC naive, no microseconds)
    # =========================
    for r in rows:
        v = r.get("data")
        dt = None

        if isinstance(v, datetime):
            dt = v
        elif isinstance(v, (int, float)):
            dt = datetime.fromtimestamp(v, tz=timezone.utc)
        else:
            s = str(v).strip()
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            try:
                dt = datetime.fromisoformat(s)
            except Exception:
                dt = None
            if dt is None:
                for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
                    try:
                        dt = datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
                        break
                    except Exception:
                        continue
            if dt is None:
                dt = datetime.now(timezone.utc)

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt = dt.astimezone(timezone.utc).replace(microsecond=0)
        r["data"] = dt.replace(tzinfo=None)  # naive UTC for DB compatibility

    w_now = time.perf_counter()
    p_now = time.process_time()
    watch = w_now - w_prev
    proc = p_now - p_prev
    w_prev = w_now
    p_prev = p_now
    steps.append({'step': str("Normalize timestamps"), 'status': str(''), 'watch': float(watch), 'proc': float(proc)})

    # =========================
    # 4) Load DB targets
    # =========================
    path = ctx["db_targets_file"]
    if not path or not os.path.exists(path):
        raise ValueError(f"Ficheiro de targets não  encontrado: {path!r}")

    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    targets_raw = raw["targets"] if isinstance(raw, dict) and "targets" in raw else raw
    if not isinstance(targets_raw, list):
        raise ValueError("db_targets.json inválido: esperado lista ou {'targets':[...]}")

    targets = []

    for i, t in enumerate(targets_raw, start=1):
        if not isinstance(t, dict):
            continue

        ttype = (t.get("type") or "postgres").strip().lower()
        name = t.get("name") or f"db{i}"

        if ttype in ("postgres", "cockroachdb", "yugabyte", "supabase", "neon", "aiven_pg", "cratedb"):
            dsn = (t.get("dsn") or "").strip()
            if not dsn and isinstance(t.get("dsn_env"), str) and t["dsn_env"].strip():
                dsn = os.getenv(t["dsn_env"].strip(), "").strip()

            table = (t.get("table") or "meteo").strip()
            if not table or not all(c.isalnum() or c == "_" for c in table):
                raise ValueError(f"Nome de tabela inválido: {table!r}")

            targets.append({
                "name": name,
                "type": "postgres" if ttype != "cratedb" else "cratedb",
                "dsn": dsn,
                "table": table
            })

        elif ttype in ("mysql", "mariadb", "tidb", "tidbcloud"):
            dsn = (t.get("dsn") or "").strip()
            if not dsn and isinstance(t.get("dsn_env"), str) and t["dsn_env"].strip():
                dsn = os.getenv(t["dsn_env"].strip(), "").strip()

            table = (t.get("table") or "meteo").strip()
            if not table or not all(c.isalnum() or c == "_" for c in table):
                raise ValueError(f"Nome de tabela inválido: {table!r}")

            targets.append({"name": name, "type": "mysql", "dsn": dsn, "table": table})

        elif ttype in ("mongodb", "mongo"):
            uri = (t.get("uri") or "").strip()
            if not uri and isinstance(t.get("uri_env"), str) and t["uri_env"].strip():
                uri = os.getenv(t["uri_env"].strip(), "").strip()

            database = (t.get("database") or "meteo").strip()
            collection = (t.get("collection") or "meteo").strip()

            targets.append({"name": name, "type": "mongodb", "uri": uri, "database": database, "collection": collection})

        else:
            raise ValueError(f"Tipo de target desconhecido ({name}): {ttype!r}")

    w_now = time.perf_counter()
    p_now = time.process_time()
    watch = w_now - w_prev
    proc = p_now - p_prev
    w_prev = w_now
    p_prev = p_now
    steps.append({'step': str(f"Load DB targets ({len(targets)})"), 'status': str(''), 'watch': float(watch), 'proc': float(proc)})

    # =========================
    # 5) Connect targets
    # =========================
    w_now = time.perf_counter()
    p_now = time.process_time()
    watch = w_now - w_prev
    proc = p_now - p_prev
    w_prev = w_now
    p_prev = p_now
    steps.append({'step': str("Starting database accesses:"), 'status': str(''), 'watch': float(watch), 'proc': float(proc)})

    for t in targets:
        name = t["name"]
        ttype = t["type"]

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

                mysql_kwargs = {"host": host, "port": port, "user": user, "password": password, "database": dbname}

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

            w_now = time.perf_counter()
            p_now = time.process_time()
            watch = w_now - w_prev
            proc = p_now - p_prev
            w_prev = w_now
            p_prev = p_now
            steps.append({'step': str(f"... connected to `{name}`"), 'status': str(''), 'watch': float(watch), 'proc': float(proc)})

        except Exception as e:
            errors[name] = str(e)
            w_now = time.perf_counter()
            p_now = time.process_time()
            watch = w_now - w_prev
            proc = p_now - p_prev
            w_prev = w_now
            p_prev = p_now
            steps.append({'step': str(f"... failed to connect to `{name}`: {e}"), 'status': str("FAIL"), 'watch': float(watch), 'proc': float(proc)})

    extra_lines.append(f"Targets ligados: {', '.join(handles.keys()) if handles else '(nenhum)'}")
    if errors:
        extra_lines.append("Targets com erro: " + "; ".join([f"{k}={v}" for k, v in errors.items()]))

    # =========================
    # 6) If no DB, offline CSV
    # =========================
    # if not handles:
    #     os.makedirs("offline_output", exist_ok=True)
    #     fp = f"offline_output/meteo_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    #     with open(fp, "w", newline="", encoding="utf-8") as f:
    #         w = csv.DictWriter(f, fieldnames=["fonte", "data", "temp", "humidade", "vento", "pressao", "precipitacao", "lugar", "lat", "lon"])
    #         w.writeheader()
    #         for r in rows:
    #             w.writerow(r)

    #     w_now = time.perf_counter()
    #     p_now = time.process_time()
    #     watch = w_now - w_prev
    #     proc = p_now - p_prev
    #     w_prev = w_now
    #     p_prev = p_now
    #     steps.append({'step': str(f"Data saved to file `{fp}`"), 'status': str(''), 'watch': float(watch), 'proc': float(proc)})
    #     extra_lines.append(f"Offline file: {fp}")

    # =========================
    # 7) Dedup + insert
    # =========================

    COLUMNS = ["fonte", "data", "temp", "humidade", "vento", "pressao", "precipitacao", "lugar", "lat", "lon"]

    # group rows by (fonte, timestamp)
    batches = {}
    for r in rows:
        fonte = r.get("fonte") or "UNKNOWN"
        ts = r.get("data")
        key = (fonte, ts)
        if key not in batches:
            batches[key] = []
        batches[key].append(r)

    # log batch count (optional)
    w_now = time.perf_counter()
    p_now = time.process_time()
    watch = w_now - w_prev
    proc = p_now - p_prev
    w_prev = w_now
    p_prev = p_now
    steps.append({'step': str(f"Batching por fonte+timestamp ({len(batches)} batches)"), 'status': str(''), 'watch': float(watch), 'proc': float(proc)})

    for target_name, h in handles.items():
        inserted = 0
        skipped = 0

        for (fonte, ts), batch_rows in batches.items():

            # get existing lugares for (fonte, ts)
            if h["type"] == "mongodb":
                q = {"fonte": fonte, "data": ts}
                docs = h["col"].find(q, {"lugar": 1, "_id": 0})
                existing_lugares = set()
                for d in docs:
                    if d.get("lugar") is not None:
                        existing_lugares.add(str(d.get("lugar")))
            else:
                table = h["table"]
                conn = h["conn"]
                sql = f"SELECT lugar FROM {table} WHERE fonte=%s AND data=%s"
                cur = conn.cursor()
                try:
                    cur.execute(sql, (fonte, ts))
                    rs = cur.fetchall() or []
                    existing_lugares = set()
                    for rr in rs:
                        if rr and rr[0] is not None:
                            existing_lugares.add(str(rr[0]))
                finally:
                    cur.close()

            # filter rows that are new by lugar
            to_insert = []
            for r in batch_rows:
                if str(r.get("lugar")) not in existing_lugares:
                    to_insert.append(r)

            skipped += (len(batch_rows) - len(to_insert))
            if not to_insert:
                continue

            # insert
            if h["type"] == "mongodb":
                now = datetime.now(timezone.utc).replace(tzinfo=None)
                docs = []
                for r in to_insert:
                    d = dict(r)
                    d["regdata"] = now
                    docs.append(d)
                h["col"].insert_many(docs, ordered=False)

            else:
                table = h["table"]
                conn = h["conn"]
                cols_sql = ", ".join(COLUMNS + ["regdata"])
                ph = ", ".join(["%s"] * len(COLUMNS) + ["CURRENT_TIMESTAMP"])
                insert_sql = f"INSERT INTO {table} ({cols_sql}) VALUES ({ph})"

                values = []
                for r in to_insert:
                    values.append(tuple(r.get(c) for c in COLUMNS))

                cur = conn.cursor()
                try:
                    cur.executemany(insert_sql, values)
                    conn.commit()
                except Exception:
                    conn.rollback()
                    raise
                finally:
                    cur.close()

            inserted += len(to_insert)

        w_now = time.perf_counter()
        p_now = time.process_time()
        watch = w_now - w_prev
        proc = p_now - p_prev
        w_prev = w_now
        p_prev = p_now
        steps.append({'step': str(f"Write summary @ {target_name}: ins={inserted}, skip={skipped}"), 'status': str(''), 'watch': float(watch), 'proc': float(proc)})

finally:
    # =========================
    # Close targets
    # =========================
    try:
        for h in handles.values():
            try:
                if h["type"] in ("postgres", "cratedb", "mysql"):
                    h["conn"].close()
                elif h["type"] == "mongodb":
                    h["client"].close()
            except Exception:
                pass

        w_now = time.perf_counter()
        p_now = time.process_time()
        watch = w_now - w_prev
        proc = p_now - p_prev
        w_prev = w_now
        p_prev = p_now
        steps.append({'step': str("Fecho targets"), 'status': str(''), 'watch': float(watch), 'proc': float(proc)})
    except Exception:
        pass

    # =========================
    # Build HTML summary
    # =========================
    subject = f"Pipeline {ctx['pipeline_name']} - Relatório de Execução ({ctx['env']})"
    header0 = os.getenv("PIPELINE_PCP_HEADER") or f"Task(s) of {ctx['pipeline_name']} ({ctx['env']})."

    w_total = 0.0
    p_total = 0.0
    rows_tbl = []
    for r in steps:
        w_total += r["watch"]
        p_total += r["proc"]
        label = r["step"]
        if r["status"]:
            label = f"{label} ({r['status']})"
        rows_tbl.append([label, round(w_total, 2), round(p_total, 2)])
    rows_tbl.append(["Overall (before email):", round(w_total, 2), round(p_total, 2)])

    if HAS_TABULATE:
        table_html = tabulate(rows_tbl, headers=[header0, "watch time (secs)", "proc time (secs)"], tablefmt="html")
        table_txt = tabulate(rows_tbl, headers=[header0, "watch time (secs)", "proc time (secs)"])
    else:
        table_html = "<table border='1' cellpadding='4' cellspacing='0'>"
        table_html += f"<tr><th>{header0}</th><th>watch time (secs)</th><th>proc time (secs)</th></tr>"
        for rr in rows_tbl:
            table_html += f"<tr><td>{rr[0]}</td><td>{rr[1]}</td><td>{rr[2]}</td></tr>"
        table_html += "</table>"
        table_txt = "\n".join([f"{rr[0]} | {rr[1]} | {rr[2]}" for rr in rows_tbl])

    extra_info = (
        f"<p>Pipeline: <b>{ctx['pipeline_name']}</b> | Ambiente: <b>{ctx['env']}</b> | "
    )

    if ctx.get("dashboard_url"):
        url = ctx["dashboard_url"]
        extra_info += f'<p>Pode verificar os dados em: <a href="{url}">{url}</a></p>'

    if extra_lines:
        extra_info += "<br>" + "<br>".join(f"<p>{line}</p>" for line in extra_lines)

    body_html = f"""
    <html>
    <body>
        <p><b>Resumo da execução da pipeline:</b></p>
        {extra_info}
        {table_html}
        <br>
        <p>Data/hora de execução (UTC): {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}</p>
    </body>
    </html>
    """

    # =========================
    # Send email (Resend -> SMTP fallback)
    # =========================
    resend_key = (os.getenv("RESEND_API_KEY") or "").strip()
    resend_from = (os.getenv("RESEND_FROM") or "").strip() or ctx["email_from"]

    sent_ok = False

    if resend_key:
        try:
            payload = {"from": resend_from, "to": ctx["email_to"], "subject": subject, "html": body_html}
            r = requests.post(
                "https://api.resend.com/emails",
                headers={"Authorization": f"Bearer {resend_key}", "Content-Type": "application/json"},
                json=payload,
                timeout=20,
            )
            r.raise_for_status()
            print("Email enviado com sucesso (Resend).")
            sent_ok = True
        except Exception as e:
            print(f"Falha ao enviar email via Resend: {e}")

    if not sent_ok:
        try:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["To"] = ", ".join(ctx["email_to"])

            smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com")
            smtp_port = int(os.getenv("SMTP_PORT", "587"))
            smtp_user = os.getenv("SMTP_USER")
            smtp_pass = os.getenv("SMTP_PASS")
            if not smtp_user or not smtp_pass:
                raise RuntimeError("Faltam SMTP_USER / SMTP_PASS.")

            email_from = os.getenv("PIPELINE_EMAIL_FROM") or smtp_user or ctx["email_from"]
            msg["From"] = email_from
            msg.attach(MIMEText(body_html, "html"))

            with smtplib.SMTP(smtp_host, smtp_port) as server:
                server.ehlo()
                server.starttls()
                server.ehlo()
                server.login(smtp_user, smtp_pass)
                server.send_message(msg)

            print("Email enviado com sucesso (SMTP).")
        except Exception as e:
            print(f"Falha ao enviar email (SMTP): {e}")

    print("\n--- SUMÁRIO PCP (texto) ---\n")
    print(table_txt)
