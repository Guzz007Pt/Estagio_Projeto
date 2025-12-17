Visão geral

Este módulo implementa uma pipeline de recolha e persistência de dados meteorológicos a partir de um endpoint REST (IPMA) para uma base de dados PostgreSQL (ou compatível), com monitorização por etapas (tempo de execução) e envio de email com sumário da execução. 

Objectivos desta 1ª iteração

Validar o fluxo “fim-a-fim”: API → parsing → BD → sumário por email. 

Criar uma estrutura de pipeline que depois possa ser evoluída para: redundância, idempotência, múltiplas bases de dados, etc. (alinhado com o logbook do estágio). 

Configuração
Variáveis principais

DB_CONFIG: parâmetros de ligação à base de dados (host, user, password, sslmode, etc.). Nota: credenciais não devem ser versionadas no repositório; idealmente devem vir de secrets / variáveis de ambiente.

API_URL: endpoint do IPMA para observações meteorológicas por estações.

EMAIL_FROM, EMAIL_TO: remetente e destinatários do sumário.

Estrutura dos dados
Formato esperado do JSON de entrada

O parse_data() assume que o JSON vem estruturado como:

chave: timestamp

valor: dicionário de estações {station_id: {campos...}} 

Formato produzido pelo parse_data()

Lista de dicionários (um por estação e timestamp), com chaves como:
fonte, data, temp, humidade, vento, vento_km, pressao, precipitacao, radiacao, id_estacao, id_direcc_vento. 

Nota importante (baseline): a query preparada em prepare_query() tenta inserir campos (lugar, lat, lon) que não são produzidos no parse_data(). Isto é uma limitação da 1ª iteração e deve ser corrigido numa evolução.

Documentação das funções
request_data(api_url)

Descrição: Faz o pedido HTTP GET ao endpoint REST e mede o tempo de execução.
Parâmetros:

api_url (str): URL do endpoint.

Retorna:

(json, elapsed) onde:

json (dict): resposta convertida via response.json()

elapsed (float): tempo decorrido em segundos

Excepções:

Propaga requests exceptions (raise_for_status(), timeouts, etc.).

Efeitos laterais: chamada de rede. 

receive_data(json_data)

Descrição: Valida se os dados recebidos são “não vazios” e mede o tempo.
Parâmetros:

json_data (qualquer): objecto devolvido pela API.

Retorna: (json_data, elapsed)
Excepções: ValueError se json_data for vazio/falso. 

parse_data(json_data)

Descrição: Percorre o JSON e transforma-o numa lista de registos normalizados (um por estação).
Parâmetros:

json_data (dict): estrutura {timestamp: {station_id: values}}.

Retorna: (parsed, elapsed)

parsed (list[dict]): lista de registos prontos para persistência (ou para enriquecer depois).

Notas:

Ignora entradas inesperadas (quando stations ou values não são dicionários).

Mantém o timestamp como string no campo data (pode ser convertido para datetime numa iteração seguinte). 

connect_db()

Descrição: Abre ligação à base de dados usando psycopg2 e mede o tempo.
Retorna: (conn, elapsed)
Excepções: Propaga erros de autenticação/ligação. 

prepare_query(parsed_data)

Descrição: Define a SQL de inserção e mede o tempo.
Parâmetros:

parsed_data: não é usado nesta iteração (pode ser removido ou usado para adaptar a query ao schema).

Retorna: (query, elapsed)

Nota (baseline):

A query actual insere em meteo(...) e usa placeholders %(lugar)s, %(lat)s, %(lon)s que não existem no output do parse_data().

execute_query(conn, query, parsed_data)

Descrição: Executa inserções em lote (executemany) e faz commit.
Parâmetros:

conn: ligação BD aberta

query (str): SQL parametrizada

parsed_data (list[dict]): registos para inserir

Retorna: elapsed (float)

Efeitos laterais: escrita na base de dados.
Excepções: Propaga erros de SQL / constraints / rede. 

close_connection(conn)

Descrição: Fecha a ligação à BD e mede o tempo.
Retorna: elapsed (float) 

send_summary_email(summary_rows)

Descrição: Gera uma tabela HTML com o resumo das etapas (status + tempos) e envia por SMTP.
Parâmetros:

summary_rows (list[list]): linhas com [Etapa, Status, Watch Time]

Efeitos laterais: envio de email (smtplib.SMTP("localhost")).
Notas:

Requer um servidor SMTP acessível em localhost (em Render/Cloud isto pode não existir; pode exigir SMTP externo). 

Pipelines
pipeline_meteo_normal(api_url)

Descrição: Executa o fluxo “API → validação → parsing → BD → insert → fecho”, registando métricas por etapa.
Comportamento em erro: adiciona linha ["Erro", "FAIL (...)", 0] e envia sempre email no finally. 

pipeline_meteo_heavy(api_url)

Descrição: Variante que abre primeiro a ligação à BD, e só depois faz pedido à API.
Objectivo: comparar comportamento/tempos e testar robustez (inclui tentativa de fechar ligação em caso de erro). 

Limitações conhecidas desta 1ª iteração (ótimo para relatares como “ponto de partida”)

Desalinhamento schema ↔ parsing: a SQL usa lugar/lat/lon mas o parsing não fornece esses campos.

Credenciais no código: deve migrar para secrets/env vars (evitar exposição e permitir execução em múltiplos contextos).

Sem idempotência/deduplicação: não há verificação de registos repetidos (no futuro: ON CONFLICT, chaves naturais, ou controlo por tstamp+estacao+fonte).