# Estágio — Pipeline Meteorologia (v0.3.1)

Este repositório documenta a evolução de uma **pipeline de recolha de dados meteorológicos via REST API** e **persistência em base(s) de dados**, com foco em **rastreabilidade**, **portabilidade**, **redundância (fail‑safe)** e **execuções idempotentes**. fileciteturn14file0

A **v0.3.1** é uma iteração incremental sobre a v0.3, trazendo:
- **Relatório “PCP-like”**: tempos **cumulativos** por etapa + *Overall*, alinhado com o estilo do orientador.
- **Mensagens de ligação por target**: `Connecting...`, `... connected`, `... failed`, e resumo de escrita por target.
- **SMTP externo (Gmail)**: envio de email via `smtp.gmail.com` com **App Password** (em vez de depender de `SMTP("localhost")`).
- **Config mais clara de targets**: `type` + `dsn_env/uri_env` (segredos no `.env`, nada hardcoded).

---

## Índice

- [O que mudou em v0.3.1](#o-que-mudou-em-v031)
- [Fluxo da pipeline](#fluxo-da-pipeline)
- [Requisitos](#requisitos)
- [Instalação](#instalação)
- [Configuração](#configuração)
  - [Variáveis de ambiente](#variáveis-de-ambiente)
  - [Ficheiro `db_targets.json`](#ficheiro-db_targetsjson)
- [Como executar](#como-executar)
- [Outputs](#outputs)
  - [Inserção em BD](#inserção-em-bd)
  - [Modo offline](#modo-offline)
  - [Email de resumo](#email-de-resumo)
  - [Relatório de execução (PCP-like)](#relatório-de-execução-pcp-like)
- [Deduplicação](#deduplicação)
- [Schema mínimo recomendado](#schema-mínimo-recomendado)
- [Mapeamento de dados](#mapeamento-de-dados)
- [Troubleshooting](#troubleshooting)
- [Próximos passos](#próximos-passos)

---

## O que mudou em v0.3.1

### ✅ Alterações principais
- **Tempo cumulativo** por etapa (em vez de “tempo do passo”) + **Overall** no fim.
- **Instrumentação por target**:
  - `Connecting to \`<name>\``
  - `... connected to \`<name>\``
  - `... failed to connect to \`<name>\`: <erro>`
  - `... <day> inserted ... @ <target>`
  - `Write summary @ <target>: ins=<n>, skip=<n>`
- **Email via SMTP externo (Gmail)** com credenciais via `.env` (App Password).
- **Env toggles** para compatibilidade/local:
  - `USE_CLTS_PCP=1` para usar `clts_pcp` (opcional).
  - `PIPELINE_PCP_HEADER=...` para header idêntico ao PCP.

---

## Fluxo da pipeline

```mermaid
flowchart LR
  A["get_context()"] --> B["Request API<br/>(request_data)"]
  B --> C["Receção/Validação JSON<br/>(receive_data)"]
  C --> D["Parsing -> registos normalizados<br/>(parse_data)"]
  D --> E["Load targets BD<br/>(load_db_targets)"]
  E --> F["Connect targets<br/>(connect_targets)"]

  F -->|0 ligações| G["Persistência offline<br/>(offline_dump -> CSV)"]
  F -->|>=1 ligação| H["Batching por fonte+dia<br/>(build_batches_by_fonte_day)"]
  H --> I["Dedup por dia (opcional)<br/>(*_exists_day)"]
  I --> J["Inserção em lote<br/>(*_insert_many)"]

  G --> K["Email c/ sumário<br/>(send_summary_email)"]
  J --> K
  K --> L["Fecho de ligações<br/>(close_targets)"]
```

---

## Requisitos

- Python **3.10+** (recomendado)
- Dependências típicas:
  - `requests`
  - `python-dotenv`
  - `pandas`
  - `tabulate`
  - **Postgres/CrateDB**: `psycopg2-binary` (ou `psycopg2`)
  - **MySQL/TiDB/MariaDB**: `pymysql` *(recomendado)* ou `mysql-connector-python`
  - **MongoDB**: `pymongo`
  - `clts-pcp` *(opcional — só se quiseres métricas PCP via lib)*

---

## Instalação

### 1) Criar ambiente virtual
```bash
python -m venv .venv
```

**Windows (PowerShell):**
```bash
.\.venv\Scripts\Activate.ps1
```

**Linux/macOS:**
```bash
source .venv/bin/activate
```

### 2) Instalar dependências
Se tiver `requirements.txt`:
```bash
pip install -r requirements.txt
```

Se não existir (exemplo “full”):
```bash
pip install requests python-dotenv pandas tabulate psycopg2-binary pymongo pymysql
```

---

## Configuração

### Variáveis de ambiente

A pipeline lê variáveis via `.env` (com `python-dotenv`) e/ou variáveis do sistema.

Cria um ficheiro `.env` na raiz do projeto (ao lado do `main.py`), por exemplo:

```env
# contexto
PIPELINE_USER=gustavo
PIPELINE_ENV=local
PIPELINE_NAME=PCP-Meteo-v0.3.1

# API (novo) - mantém compatibilidade com PIPELINE_API_URL_IPMA (antigo)
PIPELINE_API_URL=

# dedup (recomendado: date; para testar inserts: none)
PIPELINE_DEDUP_MODE=date

# targets BD
PIPELINE_DB_TARGETS_FILE=db_targets.json

# email (quem envia / quem recebe)
PIPELINE_EMAIL_FROM=gustavomaquina.pipe@gmail.com
PIPELINE_EMAIL_TO=pedro.pimenta@cm-maia.pt,gustavo.sa.martins@gmail.com
PIPELINE_DASHBOARD_URL_METEO=

# SMTP (Gmail)
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=gustavomaquina.pipe@gmail.com
SMTP_PASS=XXXX XXXX XXXX XXXX   # App Password (16 chars)

# relatório PCP-like
USE_CLTS_PCP=0
PIPELINE_PCP_HEADER=
```

> ⚠️ **Nunca** comitar `.env` com segredos. Usa `.gitignore`.

---

### Ficheiro `db_targets.json`

A v0.3.1 suporta targets no formato:
- uma lista `[...]`, ou
- um objeto `{ "targets": [...] }`

Campos recomendados por target:

- `name`: identificador amigável
- `type`: `postgres` | `mysql` | `cratedb` | `mongodb`
- `enabled` (opcional): `true/false`
- Para SQL: `table` (ex.: `"meteo"`)
- Para Postgres/CrateDB/MySQL:
  - `dsn_env` (recomendado) **ou** `dsn` (menos recomendado)
- Para MongoDB:
  - `uri_env` (recomendado) **ou** `uri`
  - `database`
  - `collection`

#### Exemplo completo (multi‑tipo)

```json
{
  "targets": [
    {
      "name": "neon_pg",
      "type": "postgres",
      "enabled": true,
      "table": "meteo",
      "dsn_env": "NEON_DSN"
    },
    {
      "name": "tidbcloud",
      "type": "mysql",
      "enabled": true,
      "table": "meteo",
      "dsn_env": "TIDB_DSN"
    },
    {
      "name": "crate",
      "type": "cratedb",
      "enabled": false,
      "table": "meteo",
      "dsn_env": "CRATE_DSN"
    },
    {
      "name": "mongo",
      "type": "mongodb",
      "enabled": true,
      "uri_env": "MONGO_URI",
      "database": "meteo",
      "collection": "observations"
    }
  ]
}
```

E no `.env`:

```env
NEON_DSN=postgresql://USER:PASS@HOST:5432/DB?sslmode=require
TIDB_DSN=mysql://USER:PASS@HOST:4000/DB
CRATE_DSN=postgresql://USER:PASS@HOST:5432/DB?sslmode=require
MONGO_URI=mongodb+srv://USER:PASS@HOST/?retryWrites=true&w=majority
```

> Dica: para desativar um target sem apagar nada, usa `"enabled": false`.

---

## Como executar

Na raiz do projeto:

```bash
python main.py
```

O script:
1. lê contexto,
2. chama a API,
3. normaliza os dados,
4. lê `db_targets.json`,
5. tenta ligar a todos os targets (com *fail‑safe*),
6. faz *batching* e dedup (se ativo),
7. escreve em 1+ bases (ou guarda CSV offline),
8. envia email de resumo,
9. fecha ligações.

### Teste rápido: só email (sem correr o resto)
```bash
python -c "from main import get_context, StepMonitor, send_summary_email; ctx=get_context(); mon=StepMonitor(ctx); mon.mark('Local email test',seconds_override=0.2); send_summary_email(ctx, mon, extra_lines=['Hello from local test'])"
```

---

## Outputs

### Inserção em BD

A inserção usa um `INSERT` para a tabela `meteo` (ou equivalente), com subset mínimo:

- `fonte`
- `data`
- `temp`
- `humidade`
- `vento`
- `pressao`
- `precipitacao`
- `lugar`
- `lat`
- `lon`
- `regdata = NOW()` *(quando aplicável)*

### Modo offline

Se **nenhuma BD** estiver acessível, é gerado um CSV em:

- `offline_output/meteo_YYYYMMDD_HHMMSS.csv`

### Email de resumo

O email inclui:
- identificação (pipeline/env/user),
- opcionalmente link de dashboard (`PIPELINE_DASHBOARD_URL_METEO`),
- tabela HTML com os passos e tempos cumulativos,
- linhas extra (ex.: DBs ligadas e DBs com erro).

Na v0.3.1, o envio é via **SMTP externo**:

- host/port: `SMTP_HOST` / `SMTP_PORT`
- auth: `SMTP_USER` + `SMTP_PASS` (**App Password** no caso do Gmail)

### Relatório de execução (PCP-like)

- Mostra **tempo cumulativo** por etapa (*watch* e *proc*).
- Permite configurar o cabeçalho com `PIPELINE_PCP_HEADER`.
- Por default usa `time.perf_counter()` / `time.process_time()` para evitar “tudo 0”.
- Se quiseres usar `clts_pcp`, ativa `USE_CLTS_PCP=1`.

---

## Deduplicação

Modo recomendado: `PIPELINE_DEDUP_MODE=date`

- faz “exists check” por **(fonte, dia)** antes de inserir,
- se existir, marca como `skipped (dedup)`.

Para testes em que queres sempre inserir:
```env
PIPELINE_DEDUP_MODE=none
```

---

## Schema mínimo recomendado

Se precisares de criar rapidamente uma tabela compatível (mínimo):

```sql
CREATE TABLE IF NOT EXISTS meteo (
  id SERIAL PRIMARY KEY,
  fonte VARCHAR(30),
  data TIMESTAMP,
  temp REAL,
  humidade REAL,
  vento REAL,
  pressao REAL,
  precipitacao REAL,
  lugar VARCHAR(80),
  lat REAL,
  lon REAL,
  regdata TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

---

## Mapeamento de dados

A pipeline normaliza várias fontes para um formato comum (mínimo):

| Campo normalizado | Significado | Nota |
|---|---|---|
| `fonte` | origem (ex. `Weatherbit`, `IPMA`) | string |
| `data` | timestamp/observação | `timestamp` |
| `temp` | temperatura | pode vir `None` |
| `humidade` | humidade relativa | pode vir `None` |
| `vento` | intensidade do vento | pode vir `None` |
| `pressao` | pressão | pode vir `None` |
| `precipitacao` | precipitação | pode vir `None` |
| `lugar` | identificador lógico | útil para dedup |
| `lat/lon` | coordenadas | nem sempre disponíveis |

---

## Troubleshooting

### `getaddrinfo failed`
- Normalmente `SMTP_HOST` ou `DB host` mal preenchido.
- Para Gmail, `SMTP_HOST` tem de ser **`smtp.gmail.com`** (não é email).

### `535 Username and Password not accepted`
- Estás a usar a password normal do Gmail.
- Tens de usar **App Password** (com 2FA ativo).

### “Falha a ligar” com `HOST` / `host`
- O DSN/URI está com placeholders.
- Preenche o `dsn_env` e define a env var no `.env`.

---

## Próximos passos

- Normalizar suporte SSL/TLS para MySQL/TiDB (ex.: `ssl_ca` via env).
- Melhorar idempotência (ex.: `UNIQUE` + `ON CONFLICT DO NOTHING` / upserts).
- Logging estruturado (níveis, timestamps, correlation id por execução).
- Automatizar validação de `db_targets.json` (schema + sanity checks).
