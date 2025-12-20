# Pipeline Meteo (Weatherbit/IPMA → PostgreSQL multi-target / CSV) — v0.3

Pipeline para:
1. Recolher observações meteorológicas (**Weatherbit** ou **IPMA**)
2. Transformar JSON → registos normalizados (`parsed_data`)
3. Persistir em **PostgreSQL** (modo online, com **fail-safe multi-DB**) ou em **CSV** (modo offline)
4. Evitar duplicados (checker SQL)
5. Enviar email com sumário (tempos por etapa + OK/FAIL) e, quando disponível, tabela HTML com métricas

---

## Índice

- [Visão geral](#visão-geral)
- [Iterações e progressão](#iterações-e-progressão)
- [Requisitos](#requisitos)
- [Configuração](#configuração)
  - [Variáveis de ambiente](#variáveis-de-ambiente)
  - [db_targets.json](#db_targetsjson)
  - [Email](#email)
- [Como executar](#como-executar)
- [Modo offline](#modo-offline)
- [Fluxo da pipeline](#fluxo-da-pipeline)
- [Tabela alvo e modelo de dados](#tabela-alvo-e-modelo-de-dados)
  - [Registo normalizado](#registo-normalizado)
  - [Mapeamento Weatherbit](#mapeamento-weatherbit)
  - [Mapeamento IPMA](#mapeamento-ipma)
- [Deduplicação](#deduplicação)
- [Funções principais](#funções-principais)
- [Limitações conhecidas](#limitações-conhecidas)
- [Próximos passos](#próximos-passos)

---

## Visão geral

A v0.3 introduz melhorias de robustez e rastreabilidade:

- **API dual (Weatherbit / IPMA)**  
  - Weatherbit quando existe `WEATHERBIT_API_KEY`
  - IPMA como fallback
- **Persistência online com fail-safe multi-DB**  
  - tenta ligar a múltiplas bases de dados e insere nas que estiverem disponíveis
- **Persistência offline (CSV)**  
  - se nenhuma BD estiver disponível, grava CSV em `offline_output/`
- **Deduplicação**  
  - antes de inserir, corre um “checker SQL” para evitar duplicados por chave lógica
- **Email de sumário**  
  - tabela HTML com estados/tempos por etapa (se SMTP estiver configurado)

---

## Iterações e progressão

- v0.1 — baseline IPMA → `docs/iterations/v0.1/`
- v0.1.1 — modo offline (CSV) → `docs/iterations/v0.1.1/`
- v0.1.2 — mudança de API + ajuste de query → `docs/iterations/v0.1.2/`
- v0.2.0 — context-aware + online/offline automático → `docs/iterations/v0.2.0/`
- **v0.3 — API dual + multi-DB fail-safe + deduplicação + email/monitor** → `docs/iterations/v0.3/`

---

## Requisitos

- Python 3.10+
- Dependências:
  - `requests`
  - `psycopg2-binary`
  - `tabulate`
  - `python-dotenv`
  - `pandas`

Instalação (mínimo):

```bash
pip install requests psycopg2-binary tabulate python-dotenv pandas

Configuração
Variáveis de ambiente

Recomendado: usar .env local (não commitar). Mantém um .env.example no repo.

Pipeline

    PIPELINE_NAME (ex: GM-METEO)

    PIPELINE_ENV (ex: local, dev, prod)

    PIPELINE_USER (ex: gustavo)

    PIPELINE_API_URL (opcional: força o endpoint)

    PIPELINE_API_URL_IPMA (opcional: força IPMA)

    PIPELINE_DASHBOARD_URL_METEO (opcional)

Weatherbit

    WEATHERBIT_API_KEY

    WEATHERBIT_CITY (ex: Maia,PT)

Fallback DB por env (se não existir db_targets.json)

    DB_HOST

    DB_PORT

    DB_NAME

    DB_USER

    DB_PASSWORD

    DB_SSLMODE (ex: require)

    Boas práticas: nunca commitar .env.

db_targets.json

A v0.3 pode usar múltiplos targets (fail-safe). Em vez de uma BD só, defines uma lista de targets.

Importante: db_targets.json não deve ser commitado (contém segredos).

    adiciona ao .gitignore: db_targets.json

    versiona apenas um db_targets.example.json

Exemplo (db_targets.example.json):

{
  "targets": [
    {
      "name": "db_main",
      "host": "HOST_AQUI",
      "port": 26257,
      "dbname": "DB_AQUI",
      "user": "USER_AQUI",
      "password": "PASSWORD_AQUI",
      "sslmode": "require"
    },
    {
      "name": "db_backup",
      "host": "HOST_BACKUP_AQUI",
      "port": 26257,
      "dbname": "DB_BACKUP_AQUI",
      "user": "USER_AQUI",
      "password": "PASSWORD_AQUI",
      "sslmode": "require"
    }
  ]
}

Email

Por defeito, o envio usa SMTP em localhost. Se não tiveres SMTP local:

    configurar SMTP externo (host/porta/auth), ou

    desativar envio de email no ambiente de dev.

Variáveis lógicas:

    PIPELINE_EMAIL_FROM

    PIPELINE_EMAIL_TO (emails separados por vírgula)

    Recomendação: não manter destinatários reais hardcoded.

Como executar

python main.py

O __main__ executa por defeito a variante “normal”.
Modo offline

Se a pipeline não conseguir ligar a nenhuma BD, entra em modo offline e grava CSV:

    pasta: offline_output/

    ficheiro: meteo_YYYYMMDD_HHMMSS.csv

Fluxo da pipeline

flowchart LR
  A["Definir contexto (env/.env)"] --> B["Escolher API (Weatherbit/IPMA)"]
  B --> C["Request API"]
  C --> D["Receção/Validação JSON"]
  D --> E["Parsing JSON → parsed_data"]
  E --> F["Carregar targets BD (db_targets.json / env)"]
  F --> G["Ligar a múltiplas BD (fail-safe)"]
  G --> H["Checker: deduplicação (por registo)"]
  H --> I["Insert (BDs disponíveis)"]
  I --> J["Se 0 BDs → dump CSV (offline_output/)"]
  J --> K["Fecho ligações"]
  K --> L["Resumo: terminal + email HTML"]

Tabela alvo e modelo de dados
Tabela alvo (modo online)

Nesta versão, a persistência assume uma tabela meteo (ou equivalente) com colunas:

    fonte, data, temp, humidade, vento, pressao, precipitacao

    lugar, lat, lon

    regdata (timestamp de registo, ex: NOW())

    A tabela ipma_obs pode continuar a existir como objetivo do logbook, mas o estado atual (v0.3) usa um modelo mínimo (meteo) para suportar ambas as fontes (Weatherbit/IPMA).

Registo normalizado

Independentemente da fonte, a pipeline produz registos com:

    fonte

    data

    temp

    humidade

    vento

    pressao

    precipitacao

    lugar

    lat

    lon

Mapeamento Weatherbit

    fonte → "WEATHERBIT"

    data → ob_time

    temp → temp

    humidade → rh

    vento → wind_spd

    pressao → pres

    precipitacao → precip

    lugar → city_name (ou WEATHERBIT_CITY)

    lat/lon → lat/lon

Mapeamento IPMA

    fonte → "IPMA"

    data → timestamp (top-level key)

    temp → temperatura

    humidade → humidade

    vento → intensidadeVento

    pressao → pressao

    precipitacao → precAcumulada

    lugar → station_id (usado como identificador por estação)

    lat/lon → None (ainda sem enrichment nesta iteração)

Deduplicação

Antes de inserir, a pipeline corre um “checker” para saber se o registo já existe.

Chave lógica recomendada (v0.3):

    fonte

    data

    lugar

Exemplo de checker SQL (referência):

SELECT 1
FROM meteo
WHERE fonte = %(fonte)s
  AND data = %(data)s
  AND lugar = %(lugar)s
LIMIT 1;

Se existir, o registo é contado como skip; caso contrário, é insert.
Funções principais

Nesta v0.3, a pipeline inclui (alto nível):

    Context/config

        carregar .env e ler variáveis (PIPELINE_*, DB_*, WEATHERBIT_*)

    API

        request/validate/parse (Weatherbit ou IPMA)

    DB

        carregar targets (db_targets.json ou fallback por env)

        ligar a múltiplos targets (fail-safe)

        checker de deduplicação + insert

    Offline

        dump CSV se nenhuma BD estiver disponível

    Monitor/relatório

        tempos por etapa + status

        email HTML (se SMTP configurado)

    Para documentação por versão: ver docs/iterations/v0.3/ (notes/mapping/evidence + code.diff).

Limitações conhecidas

    Email: por defeito depende de SMTP local (localhost).

    Segredos:

        nunca commitar .env

        nunca commitar db_targets.json com passwords

    Deduplicação: chave (fonte, data, lugar) é simples; pode ser melhorada conforme schema final.

    Performance: inserção linha-a-linha é funcional, mas pode ser otimizada (batch inserts).

    Enrichment IPMA: lat/lon/local de estação ainda não são enriquecidos nesta iteração.

Próximos passos

    Inserção em batch por DB + melhor agregação de contadores (ins/skip por target)

    Enrichment IPMA (nome da estação, lat/lon) via endpoint/tabela de estações

    Tornar email opcional por flag/env (ex: PIPELINE_SEND_EMAIL=0/1)

    Evoluir schema para suportar mais campos Weatherbit (UV, clouds, condição, etc.)

    Idempotência mais forte (ON CONFLICT / unique constraints)