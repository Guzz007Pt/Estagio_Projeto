Pipeline simples (prova de conceito) para:

recolher observações meteorológicas do IPMA (REST API),

transformar o JSON para registos normalizados,

inserir numa base de dados PostgreSQL (ou compatível),

enviar um email com sumário (tempos por etapa + OK/FAIL).

Nota: Esta é a versão “baseline”. A ideia do estágio é documentar bem esta versão e, nas iterações seguintes, justificar cada alteração (porquê, o que mudou, impacto).

Indice

Visao geral

Requisitos

Configuracao

Como executar

Fluxo da pipeline

Tabela alvo ipma_obs

Funcoes (API interna)

Limitacoes conhecidas (baseline)

Proximos passos

Visao geral

O script implementa duas variantes:

pipeline_meteo_normal(): faz primeiro o pedido à API e só depois liga à BD.

pipeline_meteo_heavy(): liga à BD primeiro e só depois chama a API (útil para comparar comportamento/tempos).

No fim, em ambos os casos:

imprime a tabela de execução no terminal,

envia um email com o resumo em HTML.

Requisitos

Python 3.10+ (recomendado)

Dependências:

requests

psycopg2 (ou psycopg2-binary)

tabulate

Acesso a uma base de dados PostgreSQL (ou compatível)

Um servidor SMTP acessível em localhost (ou ajustar para SMTP externo)

Instalação:

pip install requests psycopg2-binary tabulate

Configuracao
API

Por omissão, usa:

API_URL = https://api.ipma.pt/open-data/observation/meteorology/stations/observations.json

Base de dados

A ligação é feita via DB_CONFIG.

Boa prática: não guardar credenciais no repositório. Em iterações seguintes, mover para variáveis de ambiente / secrets.

Exemplo (recomendado):

export DB_NAME="..."
export DB_USER="..."
export DB_PASSWORD="..."
export DB_HOST="..."
export DB_PORT="26257"
export DB_SSLMODE="require"

Email

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
  A[Request API] --> B[Recepcao/Validacao JSON]
  B --> C[Parsing JSON -> lista de registos]
  C --> D[Ligacao BD]
  D --> E[Preparacao da query]
  E --> F[Execucao da query (executemany + commit)]
  F --> G[Fecho da ligacao]
  G --> H[Email com sumario + print da tabela]


As métricas por etapa (tempo em segundos) são guardadas num summary com o formato:

[Etapa, Status, Watch Time (s)]

Tabela alvo ipma_obs

No logbook partilhado existe a tabela ipma_obs com o seguinte schema:

Coluna	Tipo
fonte	varchar(30)
created	timestamp (DEFAULT CURRENT_TIMESTAMP)
time	timestamp
idestacao	varchar(30)
localestacao	varchar(30)
intensidadeventokm	real
intensidadevento	real
descdirvento	varchar(5)
temperatura	real
pressao	real
humidade	real
precacumulada	real
iddireccvento	int
radiacao	real
latitude	real
longitude	real
Mapeamento (observations.json -> ipma_obs)

Campos típicos que já tens no parsing:

fonte -> "IPMA"

time -> timestamp (chave do JSON)

idestacao -> station_id

intensidadevento -> intensidadeVento

intensidadeventokm -> intensidadeVentoKM

temperatura -> temperatura

pressao -> pressao

humidade -> humidade

precacumulada -> precAcumulada

iddireccvento -> idDireccVento

radiacao -> radiacao

Campos que não vêm no JSON de observações (na baseline) e exigem enriquecimento:

localestacao, descdirvento, latitude, longitude

Sugestão para iteração seguinte: fazer um “lookup” de metadados de estações (ou manter uma tabela auxiliar) para preencher localestacao/latitude/longitude, e mapear iddireccvento para descdirvento.

Query sugerida (para alinhar com ipma_obs)

Quando fores alinhar o código com ipma_obs, a inserção pode ser nesta forma:

INSERT INTO ipma_obs (
  fonte, time, idestacao,
  intensidadeventokm, intensidadevento,
  temperatura, pressao, humidade, precacumulada,
  iddireccvento, radiacao,
  localestacao, descdirvento, latitude, longitude
)
VALUES (
  %(fonte)s, %(time)s, %(idestacao)s,
  %(intensidadeventokm)s, %(intensidadevento)s,
  %(temperatura)s, %(pressao)s, %(humidade)s, %(precacumulada)s,
  %(iddireccvento)s, %(radiacao)s,
  %(localestacao)s, %(descdirvento)s, %(latitude)s, %(longitude)s
);

Funcoes (API interna)
request_data(api_url)

O que faz: GET ao endpoint REST e devolve JSON + tempo.

Retorna: (json_dict, elapsed_s)

Erros: exceptions de rede/HTTP.

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

Em erro: adiciona ["Erro", "FAIL (...)", 0].

Finalmente: envia email e imprime a tabela.

pipeline_meteo_heavy(api_url)

O que faz: igual à normal, mas liga à BD primeiro.

Em erro: tenta fechar a ligação se já existir.

Finalmente: envia email e imprime a tabela.

Limitacoes conhecidas (baseline)

Desalinhamento parsing ↔ SQL / tabela alvo

O teu parsing está mais próximo de ipma_obs, mas a query actual aponta para meteo e usa campos que não existem no parsed (ex: lugar, lat, lon).

Credenciais no código

Devem passar para env vars/secrets para permitir portabilidade e partilha segura.

Sem idempotencia / deduplicacao

Se correres várias vezes, podes inserir duplicados (futuro: ON CONFLICT, chaves naturais, etc.).

Dependencia de SMTP local

Em cloud, localhost pode não ter SMTP.

Proximos passos

Alinhar parse_data() + prepare_query() com a tabela ipma_obs

Enriquecimento: obter localestacao/lat/lon via metadados das estações

Migrar configuração para env vars/secrets e retirar credenciais do código

Implementar deduplicação / idempotência (ex: ON CONFLICT)

Melhorar logging e rastreabilidade (endpoint, destino, contexto de execução)