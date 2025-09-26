# Next Steps for Cartola Analytics

## Imediato
- Automatizar transformações adicionais (ex.: rodadas, partidas) seguindo o padrão do schema YAML.
- Integrar `cartola-fetch` com Prefect para encadear coleta + transformação + validação.
- Configurar monitoramento de falhas da CLI (ex.: alerta via Slack/Email quando retorno for 1).

## Médio prazo
- Definir camada `data/processed/rodadas` oficialmente (schema + transformação) para suportar vínculos das demais tabelas.
- Criar data dictionary em `docs/data-dictionary/` sincronizado com os arquivos YAML.
- Implementar testes de contrato validando que Parquets gerados aderem ao schema (tipos, PK, enum).

## Longo prazo
- Avaliar Dagster para assets complexos/lineage se o número de datasets crescer.
- Disponibilizar dashboards ou API interna consumindo `data/processed` (ex.: DuckDB, Streamlit, Superset).
- Implantar verificação de qualidade contínua (Great Expectations ou Pandera) com relatórios periódicos.
