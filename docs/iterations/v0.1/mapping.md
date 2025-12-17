# Mapeamento & estado do schema — v0.1

## Situação atual (v0.1)
A pipeline faz parsing das observações do IPMA para uma lista de dicionários e insere numa tabela da base de dados.
Na baseline, o output do parsing e a query de inserção podem não estar totalmente alinhados (mismatch de nomes de campos).

## Output do parsing (baseline)
Os registos gerados incluem (tipicamente):
- fonte
- data/time (timestamp)
- id_estacao
- id_direcc_vento
- temp/temperatura
- humidade
- pressao
- precipitacao (precAcumulada)
- vento / vento_km
- radiacao

## Tabela/query alvo (baseline)
- A inserção baseline aponta para uma tabela (ex: `meteo`) usando colunas que podem incluir:
  - lugar, lat, lon (não presentes no parsing baseline)
Isto pode causar falhas de inserção ou valores em falta.

## Alinhamento planeado (próxima iteração)
- Alinhar a nomenclatura com a tabela alvo `ipma_obs`:
  - `time`, `idestacao`, `temperatura`, `humidade`, etc.
- Adicionar enriquecimento para:
  - `localestacao`, `latitude`, `longitude` (não presentes no endpoint de observações)
- Opcionalmente derivar:
  - `descdirvento` a partir de `iddireccvento`
