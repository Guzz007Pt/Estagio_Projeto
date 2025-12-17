# Iteração 2 (v0.1.1) — Modo offline (persistência em CSV)

## Objetivo
Permitir demonstrar e validar a pipeline mesmo quando não existe acesso à base de dados.
Nesta iteração, a persistência passa a ser feita em CSV (output offline), mantendo o restante fluxo da pipeline.

## O que mudou
- A ligação à base de dados passou a ser simulada:
  - `connect_db()` devolve `None` e imprime que a ligação está offline.
  - `close_connection()` não fecha nada (no-op).
- A etapa de “execução de query” foi substituída por persistência offline:
  - `execute_query()` grava `parsed_data` num CSV (via pandas) em `offline_output/` com timestamp no nome do ficheiro.
- Mantém-se a definição da query SQL em `prepare_query()` para futura reactivação do modo online.

## Porque foi feito
- Sem acesso à BD, a pipeline não conseguia demonstrar o ciclo completo.
- O CSV permite:
  - evidenciar o resultado do parsing,
  - guardar provas da execução,
  - facilitar validação manual e comparação entre execuções.

## Impacto
- A pipeline continua executável “end-to-end” (API → parsing → persistência → sumário).
- A persistência deixa de depender da conectividade à BD nesta fase.
- Gera-se um artefacto local (CSV) reutilizável para testes e documentação.

## Limitações conhecidas (nesta iteração)
- Não há escrita na BD (apenas output offline).
- A SQL em `prepare_query()` pode não estar alinhada com os campos do parsing (permanece como referência).
- O envio de email continua a depender de SMTP em localhost.
- É necessário `pandas` para gerar o CSV.

## Próximos passos
- Reintroduzir modo online quando existir acesso à BD (ou configurar BD alternativa).
- Alinhar definitivamente os campos do parsing com o schema alvo (ex: `ipma_obs`) e implementar idempotência.
