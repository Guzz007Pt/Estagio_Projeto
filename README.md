# Estágio — Pipeline Meteorologia (v0.4)

Pipeline para:
- recolher dados meteorológicos via **REST API** (provider selecionado por `.env`)
- normalizar para um **registo canónico (core)**
- **deduplicar por timestamp** antes de inserir
- persistir em **várias bases de dados** (Postgres/MySQL/CrateDB/MongoDB)
- enviar **email de resumo** (Resend ou SMTP)

---

## Índice

- [Arquitetura](#arquitetura)
- [Requisitos](#requisitos)
- [Instalação](#instalação)
- [Configuração](#configuração)
  - [Variáveis de ambiente](#variáveis-de-ambiente)
  - [Ficheiro `db_targets.json`](#ficheiro-db_targetsjson)
- [Como executar](#como-executar)
- [Deduplicação](#deduplicação)
- [Schema mínimo recomendado](#schema-mínimo-recomendado)
- [Mapeamento de dados](#mapeamento-de-dados)
- [Extensibilidade](#extensibilidade)
  - [Adicionar uma nova BD (target)](#adicionar-uma-nova-bd-target)
  - [Adicionar uma nova API (provider)](#adicionar-uma-nova-api-provider)
  - [Adicionar colunas novas](#adicionar-colunas-novas)
    - [Novo campo **extra** (opcional)](#novo-campo-extra-opcional)
    - [Novo campo **core** (obrigatório)](#novo-campo-core-obrigatório)
- [Troubleshooting](#troubleshooting)

---

## Arquitetura

```mermaid
flowchart TD
  A[Config (.env + db_targets.json)] --> B[Request API (provider)]
  B --> C[Parse -> registo canónico (core)]
  C --> D[Normalize timestamps (UTC, sem microseconds)]
  D --> E[Load + connect targets]
  E --> F[Batching por (fonte, timestamp)]
  F --> G[Dedup por (fonte, data, lugar) em cada target]
  G --> H[Insert (core + regdata + extras opcional)]
  H --> I[Relatório PCP-like + Email]
```

---

## Requisitos

- Python 3.10+
- Dependências (conforme targets que usas):
  - `requests`
  - `python-dotenv` (opcional)
  - `psycopg2-binary` (Postgres / CrateDB via wire protocol)
  - `pymysql` ou `mysql-connector-python` (MySQL/MariaDB/TiDB)
  - `pymongo` (MongoDB)
  - `tabulate` (opcional, para HTML/texto do relatório)

---

## Instalação

```bash
pip install -r requirements.txt
# ou manual:
pip install requests python-dotenv psycopg2-binary pymysql mysql-connector-python pymongo tabulate
```

---

## Configuração

### Variáveis de ambiente

Recomendação para GitHub:
- cria um `.env` local (não commit)
- commita um `.env.example` com placeholders

Exemplo (mínimo funcional):

```env
PIPELINE_NAME=GM-METEO
PIPELINE_ENV=local
PIPELINE_USER=gustavo

# provider: weatherbit | ipma | icao
PIPELINE_API_PROVIDER=icao

# só se provider=icao
ICAO_CODE=LPPR

# targets
PIPELINE_DB_TARGETS_FILE=db_targets.json

# extras (opcional): nome da coluna SQL para JSON/texto com extras
# deixa vazio se não queres extras por default em SQL
PIPELINE_SQL_EXTRAS_COLUMN=extras

# email (opcional)
PIPELINE_EMAIL_FROM=estagio.pipeline@example.com
PIPELINE_EMAIL_TO=you@example.com
```

**Notas:**
- `PIPELINE_API_PROVIDER` **não tem fallback**: se falhar, a execução falha (mensagem clara).
- Se definires `PIPELINE_API_URL`, ele faz override ao URL automático do provider (útil para testes).

---

### Ficheiro `db_targets.json`

O ficheiro indica **para onde escrever**. Cada target tem:
- `name`: nome amigável
- `type`: `postgres` | `mysql` | `cratedb` | `mongodb`
- `dsn_env` / `uri_env`: nome da variável de ambiente com a credencial
- `table` ou `database/collection`
- `extras_column` (opcional): override por target (ex.: só alguns SQL aceitam extras)

Exemplo:

```json
{
  "targets": [
    { "name": "cockroach", "type": "postgres", "dsn_env": "COCKROACH_DSN", "table": "meteo", "extras_column": "extras" },
    { "name": "mariadb",   "type": "mysql",    "dsn_env": "MARIADB_DSN",   "table": "meteo", "extras_column": "" },
    { "name": "mongodb",   "type": "mongodb",  "uri_env": "MONGO_URI",     "database": "meteo", "collection": "meteo" }
  ]
}
```

**Regra simples para extras em SQL:**
- Se `extras_column` **for string vazia** → esse target **não recebe** extras.
- Se `extras_column` não existir → usa o default `PIPELINE_SQL_EXTRAS_COLUMN`.

---

## Como executar

```bash
python main.py
```

---

## Deduplicação

A pipeline faz dedup **apenas por timestamp**, com esta lógica:

1. agrupa linhas por **(fonte, data)**  
2. para cada target:
   - busca os `lugar` já existentes para `(fonte, data)`
   - insere apenas os `lugar` novos

Isto torna a execução **idempotente** (re-executar não duplica), desde que:
- o timestamp normalizado seja consistente (UTC, sem microseconds)
- o `lugar` seja estável (ex.: estação / cidade / ICAO)

> Dica: para máxima segurança contra concorrência, cria um índice/constraint UNIQUE em `(fonte, data, lugar)`.

---

## Schema mínimo recomendado

### Core (comum a todos)

Campos usados no **core**:

- `fonte` (texto)
- `data` (timestamp)
- `temp` (float)
- `humidade` (float)
- `vento` (float)
- `pressao` (float)
- `precipitacao` (float)
- `lugar` (texto)
- `lat` (float)
- `lon` (float)
- `regdata` (timestamp de inserção)

### SQL (Postgres)

```sql
CREATE TABLE meteo (
  fonte TEXT NOT NULL,
  data  TIMESTAMP NOT NULL,
  temp  DOUBLE PRECISION NULL,
  humidade DOUBLE PRECISION NULL,
  vento DOUBLE PRECISION NULL,
  pressao DOUBLE PRECISION NULL,
  precipitacao DOUBLE PRECISION NULL,
  lugar TEXT NOT NULL,
  lat DOUBLE PRECISION NULL,
  lon DOUBLE PRECISION NULL,
  extras JSONB NULL,
  regdata TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE (fonte, data, lugar)
);
```

### SQL (MySQL/MariaDB)

```sql
CREATE TABLE meteo (
  fonte VARCHAR(32) NOT NULL,
  data  DATETIME NOT NULL,
  temp DOUBLE NULL,
  humidade DOUBLE NULL,
  vento DOUBLE NULL,
  pressao DOUBLE NULL,
  precipitacao DOUBLE NULL,
  lugar VARCHAR(128) NOT NULL,
  lat DOUBLE NULL,
  lon DOUBLE NULL,
  extras JSON NULL,
  regdata TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uq_meteo (fonte, data, lugar)
);
```

> Se o teu MySQL não suportar `JSON`, usa `TEXT` e guarda `json.dumps(...)`.

### MongoDB

Sem schema fixo. Recomenda-se um índice:

```js
db.meteo.createIndex({ fonte: 1, data: 1, lugar: 1 }, { unique: true })
```

---

## Mapeamento de dados

O parser converte a resposta da API num registo canónico:

| Campo core | Weatherbit | IPMA | ICAO/METAR |
|---|---|---|---|
| `temp` | `temp` | `temperatura` | `tempC` (ou parse do METAR) |
| `humidade` | `rh` | `humidade` | calculada (temp/dewpoint) se possível |
| `vento` | `wind_spd` | `intensidadeVento` | knots → m/s |
| `pressao` | `pres` | `pressao` | `Q####` / `A####` |
| `precipitacao` | `precip` | `precAcumulada` | (normalmente n/a) |
| `lugar` | `city_name` | `station_id` | `ICAO_CODE` |

---

## Extensibilidade

### Adicionar uma nova BD (target)

1. **Adiciona um target** ao `db_targets.json`:
   - define `type`, `dsn_env/uri_env`, `table`/`collection`
2. **Cria a variável no `.env`** com o DSN/URI (ou mete no CI/Secrets).
3. Garante que tens a lib instalada:
   - Postgres/CrateDB → `psycopg2-binary`
   - MySQL → `pymysql` ou `mysql-connector-python`
   - MongoDB → `pymongo`

> Não precisas alterar a lógica de dedup/insert: cada target segue o mesmo contrato (`type`, `conn/col`, `table`, etc.).

---

### Adicionar uma nova API (provider)

O “contrato” do provider é: produzir uma lista `rows` onde cada item é um `dict` com os **campos core** (e `extras` opcional).

Passos:

1. **Config**
   - adiciona o nome à whitelist: `PIPELINE_API_PROVIDER in ("weatherbit","ipma","icao","<novo>")`
   - adiciona as variáveis necessárias no `.env` (ex.: `NEWPROVIDER_KEY`, `NEWPROVIDER_URL`)
2. **Request**
   - define o URL do novo provider (ou usa `PIPELINE_API_URL` como override)
3. **Parse**
   - cria um bloco `elif API_PROVIDER == "<novo>": ...`
   - converte resposta em `rows.append({ ... })`

Checklist rápido:
- `data` deve ser `datetime` (idealmente UTC)
- `lugar` deve existir e ser estável
- se houver campos que só existem nesta API → mete em `r["extras"]`

---

### Adicionar colunas novas

#### Novo campo extra (opcional)

Ex.: queres `feels_like`, mas só o Weatherbit fornece.

1. No parser, adiciona:
   ```python
   "extras": {"feels_like": obs.get("app_temp")}
   ```
2. Para SQL, garante que:
   - a tabela tem uma coluna (`extras` ou outra)
   - o target tem `extras_column` definido (ou `PIPELINE_SQL_EXTRAS_COLUMN` global)
3. Para MongoDB, não precisas de schema: `extras` fica no documento.

**Vantagem:** não mexes no core nem em todas as DBs — só os targets que suportam extras guardam.

---

#### Novo campo core (obrigatório)

Ex.: queres adicionar `uv_index` como campo “obrigatório”.

Checklist (tens de atualizar tudo o que “assume” o core):

1. **Parser**: o novo campo tem de existir em **todas** as APIs (ou ter fallback/valor `None`).
2. **COLUMNS**: adiciona o nome na lista de colunas core usada no insert.
3. **Schema SQL**: adiciona coluna na tabela (em todas as DBs SQL usadas).
4. **MongoDB**: sem schema obrigatório, mas convém manter consistência.

> Regra prática: se só algumas fontes têm o campo → **extras**.  
> Se queres mesmo tornar “core” → tens de garantir compatibilidade em todas as fontes/targets.

---

## Troubleshooting

- **SecretNotFoundError no Colab**: acontece se fizeres `userdata.get("X")` para um secret que não existe.  
  Solução: envolve com `try/except` ou só chama `userdata.get()` se souberes que está definido.
- **“Resposta <provider> inesperada”**: o provider está certo mas o formato mudou → revê o bloco de parse desse provider.
- **Problemas de timestamp/dedup**: confirma que `data` está a ser normalizado para UTC e sem microseconds.

---

