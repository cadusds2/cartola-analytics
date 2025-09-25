# Cartola Analytics

Plataforma open source para coletar, organizar e analisar dados do Cartola FC. Este repositorio reune pipelines de ingestao, camadas de dados tratadas e prototipos de analises que apoiam decisoes de escala e monitoramento de desempenho ao longo das rodadas.

## Objetivos
- Centralizar os endpoints publicos do Cartola FC em uma unica base historica.
- Disponibilizar metricas e indicadores de desempenho atualizados por rodada.
- Prover material de apoio (notebooks, dashboards, APIs internas) para experimentacao rapida.
- Garantir processos reprodutiveis, versionados e auditiveis.

## Roadmap inicial
1. Ingestao: criar pipelines para capturar os endpoints listados abaixo, com armazenamento bruto (`data/raw/`).
2. Camada tratada: consolidar esquemas normalizados (`data/processed/`) com controle de versoes.
3. Analises: publicar notebooks em `notebooks/` com insights sobre atletas, clubes e ligas.
4. Apps ou servicos: expor APIs internas ou dashboards consumindo as camadas tratadas.

## Estrutura sugerida
```
cartola-analytics/
|-- AGENTS.md
|-- README.md
|-- data/
|   |-- raw/
|   `-- processed/
|-- docs/
|   |-- data-dictionary/
|   `-- retrospectives/
|-- notebooks/
`-- src/
    |-- cartola_analytics/
    `-- tests/
```
A estrutura acima ainda esta em construcao. Utilize-a como referencia ao criar novos artefatos.

## Endpoints monitorados
- https://api.cartola.globo.com/mercado/status
- https://api.cartola.globo.com/atletas/mercado
- https://api.cartola.globo.com/atletas/pontuados
- https://api.cartola.globo.com/pos-rodada/destaques
- https://api.cartola.globo.com/clubes
- https://api.cartola.globo.com/posicoes
- https://api.cartola.globo.com/patrocinadores
- https://api.cartola.globo.com/partidas
- https://api.cartola.globo.com/videos
- https://api.cartola.globo.com/mercado/destaques
- https://api.cartola.globo.com/mercado/destaques/reservas
- https://api.cartola.globo.com/rodadas
- https://api.cartola.globo.com/partidas/[rodada]
- https://api.cartola.globo.com/rankings
- https://api.cartola.globo.com/ligas
- https://api.cartolafc.globo.com/esquemas

> Aviso: respeite limites de acesso e headers exigidos pela API. Utilize cache e throttling para evitar bloqueios.

## Stack pretendida
- Linguagem: Python >= 3.11
- Tooling: Poetry, Ruff, Black, Mypy, Pytest
- Armazenamento: Parquet/CSV em `data/`; suporte opcional a SQLite ou DuckDB para analises ad-hoc.
- Orquestracao (futuro): Prefect ou Dagster, a definir.

## Boas praticas
- Adotar `.env.example` com variaveis necessarias (tokens, caminhos, etc.).
- Registrar notebooks com metadata limpa (sem output pesado).
- Escrever testes e documentar premissas sempre que criar nova feature.
- Centralizar decisoes arquiteturais em `docs/decisions/`.

## Contribuindo
1. Abra issues descrevendo motivacao, escopo e resultado esperado.
2. Crie branches curtas (`feature/<tema>` ou `chore/<tema>`).
3. Mantenha commits pequenos com mensagens no imperativo.
4. Abra pull requests com contexto, screenshots ou notebooks quando aplicavel, e checklist de validacao.

## Recursos uteis
- Documentacao oficial Cartola FC: https://ge.globo.com/futebol/cartola-fc/
- Historico de pontuacao mantido pela comunidade: https://github.com/fhao/crawlers-cartola
- Prefect: https://docs.prefect.io/ e Dagster: https://docs.dagster.io/
- Guia de boas praticas em projetos de dados: https://cookiecutter-data-science.drivendata.org/

## Setup com Poetry
- Configure o ambiente executando `poetry install`.
- Use `poetry shell` para ativar o ambiente virtual quando necessario.
- Rode ferramentas via `poetry run`, por exemplo `poetry run pytest`.
---
Sinta-se a vontade para adaptar a estrutura conforme novas necessidades surgirem. Atualize este README a cada marco relevante do projeto.



