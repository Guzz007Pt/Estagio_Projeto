# Evidências — v0.1.2

## Execução (terminal)
- Colar output do `summary` ou anexar screenshot.

![Resumo no terminal](../../assets/v0.1.2_terminal_summary.png)

## Prova do output (CSV)
Nesta iteração, a persistência é feita em CSV (modo offline).
- Mostrar o caminho impresso no terminal:
  - `Dados guardados offline em: offline_output/meteo_YYYYMMDD_HHMMSS.csv`
- Mostrar 3–6 linhas do CSV (sem dados sensíveis).

### Exemplo (PowerShell)
```powershell
Get-Content offline_output\meteo_YYYYMMDD_HHMMSS.csv -TotalCount 6
