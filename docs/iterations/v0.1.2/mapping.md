# Mapeamento — v0.1.2 (Weatherbit → parsed_data → query/CSV)

## Fonte (Weatherbit)
A resposta vem em `data[]` e cada item inclui campos como:
- ob_time, city_name, country_code, temp, app_temp, rh, wind_spd, wind_dir, wind_cdir_full,
  pres, precip, uv, solar_rad, clouds, weather.description, station, sunrise, sunset, lat, lon.

## parsed_data (chaves geradas)
O `parse_data()` gera, por item:
- fonte = "Weatherbit"
- data (ob_time)
- cidade (city_name)
- pais (country_code)
- temp
- sensacao_termica (app_temp)
- humidade (rh)
- vento (wind_spd)
- vento_dir (wind_dir)
- vento_desc (wind_cdir_full)
- pressao (pres)
- precipitacao (precip)
- uv
- radiacao_solar (solar_rad)
- nuvens (clouds)
- condicao (weather.description)
- estacao (station)
- nascer_sol (sunrise)
- por_sol (sunset)
- lat
- lon

## Query / destino
### Modo offline
- O destino efetivo é CSV (`offline_output/meteo_<timestamp>.csv`).

### Query (referência para modo online)
- É recomendado alinhar a query com as chaves acima.
- Se a tabela continuar a ser `meteo` (mínima), sugere-se mapear:
  - `lugar` <- `cidade` (ou `"{cidade}, {pais}"`)
  - `lat` <- `lat`
  - `lon` <- `lon`
  - restantes campos básicos: fonte, data, temp, humidade, vento, pressao, precipitacao
