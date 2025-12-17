# Pipeline Meteo (IPMA → PostgreSQL) — 1ª Iteração

Pipeline simples (prova de conceito) para:
1) recolher observações meteorológicas do **IPMA** (REST API),
2) transformar o JSON para registos normalizados,
3) inserir numa base de dados **PostgreSQL** (ou compatível),
4) enviar um **email com sumário** (tempos por etapa + OK/FAIL).

> **Nota:** Esta é a versão “baseline”. A ideia do estágio é documentar muito bem esta versão e, nas iterações seguintes, justificar cada alteração (porquê, o que mudou, impacto).

---

## Índice
- [Visão geral](#visão-geral)
- [Requisitos](#requisitos)
- [Configuração](#configuração)
- [Como executar](#como-executar)
- [Fluxo da pipeline](#fluxo-da-pipeline)
- [Funções (API interna)](#funções-api-interna)
- [Limitações conhecidas (baseline)](#limitações-conhecidas-baseline)
- [Próximos passos (ideias para iterações)](#próximos-passos-ideias-para-iterações)

---

## Visão geral

O script implementa duas variantes:
- **`pipeline_meteo_normal()`**: faz primeiro o pedido à API e só depois liga à BD.
- **`pipeline_meteo_heavy()`**: liga à BD primeiro e só depois chama a API (útil para comparar comportamento/tempos).

No fim, em ambos os casos:
- imprime a tabela de execução no terminal,
- envia um email com o resumo em HTML.

---

## Requisitos

- Python 3.10+ (recomendado)
- Dependências:
  - `requests`
  - `psycopg2`
  - `tabulate`
- Acesso a uma base de dados PostgreSQL (ou compatível)
- Um servidor SMTP acessível em `localhost` (ou ajustar para SMTP externo)

Instalação:
```bash
pip install requests psycopg2-binary tabulate


Configuração
1) API

Por omissão, usa:

API_URL = https://api.ipma.pt/open-data/observation/meteorology/stations/observations.json

2) Base de dados

A ligação é feita via DB_CONFIG.

⚠️ Boa prática: não guardar credenciais no repositório.
Em iterações seguintes, mover para variáveis de ambiente / secrets.

Exemplo (recomendado):

export DB_NAME="meteo"
export DB_USER="..."
export DB_PASSWORD="..."
export DB_HOST="..."
export DB_PORT="26257"
export DB_SSLMODE="require"

3) Email

O envio usa SMTP em localhost:

EMAIL_FROM

EMAIL_TO

Se não tiveres SMTP local (ex: Render/Cloud), vais precisar de:

SMTP externo (host/porta/auth), ou

desactivar email no ambiente de dev.

Como executar
python main.py


O __main__ inicia por defeito:

pipeline_meteo_normal(API_URL)

Fluxo da pipeline
flowchart LR
  A[Request API] --> B[Recepção/Validação JSON]
  B --> C[Parsing JSON → lista de registos]
  C --> D[Ligação BD]
  D --> E[Preparação da query]
  E --> F[Execução da query (executemany + commit)]
  F --> G[Fecho da ligação]
  G --> H[Email com sumário + print da tabela]


As métricas por etapa (tempo em segundos) são guardadas num summary com o formato:

[Etapa, Status, Watch Time (s)]

Funções (API interna)

Dica: nesta 1ª iteração, documentamos “o que existe”. Nas iterações seguintes, vais actualizando esta secção com as mudanças e o racional.

request_data(api_url)

O que faz: GET ao endpoint REST e devolve JSON + tempo.

Retorna: (json_dict, elapsed_s)

Erros: exceptions de rede/HTTP (raise_for_status, timeout, etc.)

receive_data(json_data)

O que faz: valida que o JSON não está vazio.

Retorna: (json_data, elapsed_s)

Erros: ValueError se o JSON for vazio/ inválido.

parse_data(json_data)

O que faz: transforma o JSON do IPMA em lista de registos (um por estação e timestamp).

Retorna: (parsed_list, elapsed_s)

connect_db()

O que faz: cria a ligação à BD via psycopg2.

Retorna: (conn, elapsed_s)

prepare_query(parsed_data)

O que faz: define a SQL de inserção (parametrizada).

Retorna: (query_str, elapsed_s)

Nota: o parâmetro parsed_data não é usado nesta versão.

execute_query(conn, query, parsed_data)

O que faz: executemany(query, parsed_data) + commit().

Retorna: elapsed_s

close_connection(conn)

O que faz: fecha a ligação à BD.

Retorna: elapsed_s

send_summary_email(summary_rows)

O que faz: constrói uma tabela HTML (tabulate) e envia email via SMTP.

Notas: requer SMTP em localhost (ajustar em ambiente cloud).

pipeline_meteo_normal(api_url)

O que faz: executa o fluxo completo e constrói o summary.

Em erro: adiciona uma linha ["Erro", "FAIL (...)", 0].

Finalmente: envia email e imprime a tabela no terminal.

pipeline_meteo_heavy(api_url)

O que faz: igual à normal, mas liga à BD primeiro.

Em erro: tenta fechar a ligação se já existir.

Finalmente: envia email e imprime a tabela.

Limitações conhecidas (baseline)

Desalinhamento parsing ↔ SQL

O parse_data() gera campos como vento_km, radiacao, id_estacao, id_direcc_vento,

mas a SQL actual tenta inserir lugar, lat, lon (que não existem no parsed).
Resultado: a inserção pode falhar por falta de chaves no dicionário.

Credenciais no código

Devem passar para env vars/secrets para permitir portabilidade e partilha segura.

Sem idempotência / deduplicação

Se correres várias vezes, podes inserir duplicados (futuro: ON CONFLICT, chaves naturais, etc.).

Dependência de SMTP local

Em cloud, localhost pode não ter SMTP.