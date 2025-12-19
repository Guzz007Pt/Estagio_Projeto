# Iteração v0.1.2 — Mudança de API (Weatherbit) + ajuste de query

## Objetivo
Substituir a fonte de dados meteorológicos para uma nova API (Weatherbit) e adaptar o parsing e a query de inserção,
mantendo o resto da pipeline (request → validação → parsing → persistência offline → sumário).

## O que mudou
- Fonte de dados:
  - A pipeline passou a consumir a API **Weatherbit Current** (resposta com `data[]`).
- Parsing:
  - `parse_data()` foi refeito para extrair campos do Weatherbit, incluindo:
    cidade, país, temperatura, sensação térmica, humidade, vento (velocidade/direção/descrição),
    pressão, precipitação, UV, radiação solar, nuvens, condição e dados de estação/sol, latitude/longitude.
- Query:
  - A query foi ajustada para refletir o novo mapeamento de campos (mesmo estando em modo offline, mantém-se como referência).

## Porque foi feito
- Permitir testar e demonstrar a pipeline com uma fonte alternativa e mais rica em variáveis meteorológicas.
- Preparar a reativação do modo online com uma query coerente com os dados produzidos pelo parsing.

## Impacto
- A estrutura da pipeline mantém-se; o “core” muda apenas na fonte e no parsing.
- O output offline (CSV) passa a incluir novos campos que podem ser úteis para validação e análises futuras.

## Limitações / notas
- A chave da API não deve estar hardcoded (deve vir de `WEATHERBIT_API_KEY` via `.env`).
- Em modo offline, a query não é executada, mas deve manter consistência para quando o modo BD voltar.

## Próximos passos
- Normalizar tipos (ex: datas para datetime) e definir schema alvo para estes novos campos.
- Implementar modo online com BD e deduplicação/idempotência.
