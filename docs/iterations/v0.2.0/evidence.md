# Evidências — v0.2.0

## Execução (terminal)
- Anexar screenshot do resumo (ou colar output como texto).

![Resumo no terminal](../../assets/v0.2.0_terminal_summary.png)

## Prova de persistência
### Offline (CSV)
- Evidência do caminho impresso:
  - `Dados guardados offline em: offline_output/meteo_YYYYMMDD_HHMMSS.csv`
- Mostrar 3–6 linhas do CSV:

```powershell
Get-Content offline_output\meteo_YYYYMMDD_HHMMSS.csv -TotalCount 6
