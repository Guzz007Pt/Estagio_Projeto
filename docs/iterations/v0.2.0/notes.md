# Iteração v0.2.0 — Context-aware + modo online/offline automático

## Objetivo
Tornar a pipeline configurável por contexto (env vars), suportar modo online/offline automaticamente
e enriquecer o relatório por email com metadados (pipeline/env/user + link de dashboard).

## O que mudou
- Configuração por contexto (`get_context()`):
  - Lê variáveis de ambiente para definir utilizador lógico, ambiente, nome da pipeline, API URL, destinatários de email e ligação à BD.
- Modo online/offline automático:
  - `connect_db(ctx)` decide se liga à BD ou entra em modo offline (CSV), consoante o `host` existir/ser válido.
- Execução de persistência dual:
  - `execute_query(...)`:
    - se `conn is None` → grava CSV em `offline_output/` com timestamp
    - caso contrário → executa `executemany` + `commit` (modo online)
- Email com mais contexto:
  - Assunto inclui pipeline + ambiente
  - Corpo inclui pipeline/env/user e (se existir) link de dashboard.

## Porque foi feito
- Facilitar execução em múltiplos ambientes sem alterar código (apenas `.env`).
- Permitir demonstração mesmo sem acesso a BD (modo offline).
- Melhorar rastreabilidade através de emails contextualizados.

## Impacto
- Pipeline passa a ser “plug-and-play” por configuração.
- Persistência pode ser offline (CSV) ou online (PostgreSQL), mantendo o mesmo fluxo.

## Limitações / notas
- A query continua a inserir na tabela `meteo` e alguns campos como `lugar/lat/lon` ainda vêm a `None` no parsing.
- Email depende de SMTP local (pode falhar fora de ambiente configurado).

## Próximos passos
- Alinhar schema/colunas com uma tabela alvo mais completa (ex: `ipma_obs`) ou enriquecer `lugar/lat/lon`.
- Normalização de tipos (datas, números) e idempotência/deduplicação.
- Configurar SMTP externo ou tornar envio opcional por flag.
