# Iteração v0.3 — Multi-DB fail-safe + deduplicação + API dual (Weatherbit/IPMA)

## Objetivo
Tornar a pipeline mais robusta e “à prova de falhas”, suportando:
- múltiplos destinos de base de dados (fail-safe),
- deduplicação para evitar inserts repetidos em reruns,
- persistência offline em CSV quando não há BD disponível,
- compatibilidade com duas fontes (Weatherbit ou IPMA),
- relatório por email com contexto e tabela HTML (quando possível).

## O que mudou
- Fonte de dados (API) passou a ser “dual”:
  - Weatherbit (se existir `WEATHERBIT_API_KEY`)
  - IPMA (fallback)  
  A URL final é escolhida via `PIPELINE_API_URL`/`PIPELINE_API_URL_IPMA` ou por default. 
- Parsing compatível com Weatherbit OU IPMA:
  - Weatherbit retorna 1 registo normalizado
  - IPMA pode gerar vários registos (um por estação)
- Base de dados com fail-safe:
  - Suporte para múltiplos targets através de `db_targets.json`
  - Liga a todas as DBs disponíveis e reporta as que falharam
- Deduplicação:
  - Antes de inserir, corre `CHECK_DUPLICATE_SQL` (por `fonte`, `data`, `lugar`)
  - Conta inseridos vs ignorados por DB
- Persistência offline (fallback):
  - Se não houver nenhuma ligação ativa, grava CSV em `offline_output/`
- Monitorização/relatório:
  - Introdução de `StepMonitor`, usando `clts_pcp` se estiver instalado (senão fallback)
  - Email com subject contextualizado e tabela em HTML

## Porque foi feito
- A pipeline precisava de garantir execução estável mesmo com falhas de BD.
- Era necessário mostrar evolução técnica: redundância, dedupe, relatórios mais completos.
- Melhorar rastreabilidade e auditoria (CSV offline + contadores por DB).

## Impacto
- Executa “end-to-end” com ou sem BD.
- Evita inserts duplicados em reruns.
- Permite experimentar múltiplos destinos (ex: BD principal + BD de backup).
- Melhor output para documentação e validação (email + tabela + CSV).

## Limitações / notas
- `DEFAULT_EMAIL_TO` contém emails por defeito: recomenda-se forçar `PIPELINE_EMAIL_TO` e evitar defaults reais.
- `db_targets.json` pode conter credenciais: não deve ser commitado (usar `db_targets.example.json`).
- IPMA usa `lugar = station_id` para deduplicar por estação (não há lat/lon nesta iteração).
- A inserção é por linha (`insert_rows(conn, [row])`) — funciona, mas pode ser otimizado para batch.

## Próximos passos
- Otimizar inserts em batch por DB (reduzir round-trips).
- Tornar o envio de email opcional por flag/env (ex: `PIPELINE_SEND_EMAIL=0/1`).
- Enriquecer dados IPMA com local/lat/lon via tabela de estações (enrichment).
- Melhorar o “checker” (ex: dedupe por dia/intervalo, ou por chave composta mais completa).
