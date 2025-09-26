# Next Steps for Cartola Analytics

## Imediato
- Automatizar transformacoes adicionais (ex.: rodadas, partidas) seguindo o padrao do schema YAML.
- Documentar e acompanhar rollout do padrao de transformacoes automaticas (ver docs/issues/[2025Set261202][001]auto-transform-endpoints.md).
- Integrar `cartola-fetch` com Prefect para encadear coleta + transformacao + validacao.
- Configurar monitoramento de falhas da CLI (ex.: alerta via Slack/Email quando retorno for 1).

## Medio prazo
- Definir camada `data/processed/rodadas` oficialmente (schema + transformacao) para suportar vinculos das demais tabelas.
- Criar data dictionary em `docs/data-dictionary/` sincronizado com os arquivos YAML.
- Implementar testes de contrato validando que Parquets gerados aderem ao schema (tipos, PK, enum).

## Longo prazo
- Avaliar Dagster para assets complexos/lineage se o numero de datasets crescer.
- Disponibilizar dashboards ou API interna consumindo `data/processed` (ex.: DuckDB, Streamlit, Superset).
- Implantar verificacao de qualidade continua (Great Expectations ou Pandera) com relatorios periodicos.