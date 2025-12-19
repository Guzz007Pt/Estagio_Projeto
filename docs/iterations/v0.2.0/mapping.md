# Mapeamento & persistência — v0.2.0 (IPMA → parsed_data → BD/CSV)

## Fonte (IPMA observations)
Endpoint default: `.../observations.json`

Estrutura esperada:
- chave de topo: `timestamp`
- valor: dicionário `{ station_id: { ...campos... } }`

## parsed_data (campos gerados por registo)
- `fonte`: "IPMA"
- `data`: timestamp
- `temp`: temperatura
- `humidade`
- `vento`: intensidadeVento
- `vento_km`: intensidadeVentoKM
- `pressao`
- `precipitacao`: precAcumulada
- `radiacao`
- `id_estacao`
- `id_direcc_vento`
- `lugar`, `lat`, `lon` (ainda `None` nesta iteração; placeholders para compatibilidade)

## Destino (online vs offline)
### Modo offline (sem BD)
- Grava CSV em `offline_output/meteo_<timestamp>.csv`

### Modo online (com BD)
- Query de referência (tabela `meteo`):
  - `fonte, data, temp, humidade, vento, pressao, precipitacao, lugar, lat, lon, regdata`
- `regdata` é preenchido com `NOW()` na query
