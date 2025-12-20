# Mapeamento & persistência — v0.3

## 1) Seleção de fonte (API)
A pipeline pode consumir:
- Weatherbit: se existir `WEATHERBIT_API_KEY` (default `WEATHERBIT_URL`)
- IPMA: fallback (`IPMA_URL`)

A URL final pode ainda ser forçada por:
- `PIPELINE_API_URL` (preferido)
- `PIPELINE_API_URL_IPMA` (compatibilidade)

## 2) Normalização para `parsed_data`
A pipeline transforma qualquer resposta num formato comum (lista de dicts), compatível com INSERT/CSV:

### Weatherbit → 1 linha
Campos normalizados:
- `fonte`: `"WEATHERBIT"`
- `data`: `ob_time` (ou `ts` convertido)
- `temp`: `temp`
- `humidade`: `rh`
- `vento`: `wind_spd`
- `pressao`: `pres`
- `precipitacao`: `precip`
- `lugar`: `city_name` (ou fallback `WEATHERBIT_CITY`)
- `lat`: `lat`
- `lon`: `lon`

### IPMA → N linhas (uma por estação)
Para cada timestamp e estação:
- `fonte`: `"IPMA"`
- `data`: timestamp convertido
- `temp`: `temperatura`
- `humidade`: `humidade`
- `vento`: `intensidadeVento`
- `pressao`: `pressao`
- `precipitacao`: `precAcumulada`
- `lugar`: `station_id` (string) — usado para dedupe por estação
- `lat`: `None` (ainda não enriquecido)
- `lon`: `None`

## 3) Destinos (online/offline)
### Online (PostgreSQL)
Tabela alvo: `meteo`

SQL de inserção (referência):
- `fonte, data, temp, humidade, vento, pressao, precipitacao, lugar, lat, lon, regdata`

### Checker anti-duplicação
Antes de inserir, a pipeline verifica:
- `fonte`, `data`, `lugar`
e ignora se já existir registo.

## 4) Fail-safe com múltiplas DBs
Configuração por ficheiro `db_targets.json`:
- Pode ser uma lista ou `{ "targets": [...] }`
- Cada target inclui: `name`, `host`, `port`, `dbname`, `user`, `password`, `sslmode`

Se não existir ficheiro, usa fallback via env `DB_HOST/DB_PORT/...`.

> Recomendação: versionar apenas um `db_targets.example.json` e ignorar o ficheiro real no `.gitignore`.
