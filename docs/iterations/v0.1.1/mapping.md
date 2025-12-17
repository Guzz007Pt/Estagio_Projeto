# Mapeamento & estado do schema — v0.3

## Situação (v0.3)
Nesta iteração, não há inserção em base de dados: o output do parsing é guardado em CSV.
O “mapeamento” relevante aqui é:
- API (IPMA observations) → estrutura normalizada (parsed_data) → colunas do CSV.

## Colunas geradas no CSV (derivadas de parsed_data)
O CSV contém as chaves produzidas por `parse_data()`:
- fonte
- data
- temp
- humidade
- vento
- vento_km
- pressao
- precipitacao
- radiacao
- id_estacao
- id_direcc_vento

## Alinhamento com schema de BD (estado)
- A query SQL ainda existe (como referência), mas não é executada nesta iteração.
- O alinhamento com a tabela alvo (ex: `ipma_obs`) deve ser tratado numa iteração posterior, quando o modo online voltar a estar activo.
