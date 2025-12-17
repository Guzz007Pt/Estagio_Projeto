# Iteração 1 (v0.1) — Pipeline baseline

## Objetivo
Entregar uma primeira prova de conceito “end-to-end”:
IPMA REST API → validação → parsing → inserção na base de dados → resumo de execução (email + tabela no terminal).

Esta iteração foca-se em ter uma estrutura de pipeline funcional e observabilidade (tempos por etapa),
não em alinhamento perfeito com o schema ou idempotência.

## O que foi implementado
- Etapas da pipeline:
  - Pedido de dados à API (`request_data`)
  - Validação de payload não vazio (`receive_data`)
  - Parsing do JSON para uma lista de registos (`parse_data`)
  - Ligação à BD (`connect_db`)
  - Preparação da query de inserção (`prepare_query`)
  - Inserção de registos (`execute_query`)
  - Fecho da ligação à BD (`close_connection`)
  - Envio de email com resumo em HTML (`send_summary_email`)
- Duas variantes de execução:
  - `pipeline_meteo_normal`: API primeiro, BD depois
  - `pipeline_meteo_heavy`: BD primeiro, API depois
- Observabilidade básica:
  - medição de tempo (segundos) por etapa
  - estado OK/FAIL por etapa numa tabela de resumo

## Porque foram feitas estas escolhas (racional baseline)
- Manter uma pipeline linear e simples facilita debug e documentação.
- Medir o tempo por etapa dá visibilidade inicial sobre potenciais “bottlenecks”.
- O resumo por email garante visibilidade mesmo quando a execução é remota.

## Limitações conhecidas (aceites em v0.1)
- Risco de desalinhamento de schema: chaves produzidas no parsing e colunas na query SQL podem não coincidir totalmente.
- Sem deduplicação/idempotência (executar novamente pode inserir duplicados).
- Campos temporais podem ficar como strings (sem parsing/normalização rigorosa nesta fase).
- O email requer SMTP em localhost (pode não funcionar em ambiente cloud sem configuração).
- Config/credenciais possivelmente embutidas no código (deve migrar para env vars/secrets em iterações seguintes).

## Próximos passos planeados
- Alinhar o output do parsing + query de inserção com a tabela alvo (`ipma_obs`) e definir um mapeamento estável.
- Adicionar enriquecimento para metadados das estações (nome/lat/lon) se necessário.
- Definir estratégia de deduplicação (chaves únicas + `ON CONFLICT`).
- Mover configuração para variáveis de ambiente e remover segredos do código.
