# Next Steps for Cartola Analytics

## Imediato
- Integrar `cartola-fetch` com Prefect para encadear coleta + transformacao + validacao.
- Configurar monitoramento de falhas da CLI (ex.: alerta via Slack/Email quando retorno for 1).

## Medio prazo
- Emitir eventos de log padronizados (`cli_validation_<dataset>`) quando a validação automática passar/falhar.
- Documentar no `cli-guide.md` como regenerar o data dictionary e interpretar as quebras de contrato.

## Longo prazo
- Avaliar Dagster para assets complexos/lineage se o numero de datasets crescer.
- Disponibilizar dashboards ou API interna consumindo `data/processed` (ex.: DuckDB, Streamlit, Superset).
- Implantar verificacao de qualidade continua (Great Expectations ou Pandera) com relatorios periodicos.
