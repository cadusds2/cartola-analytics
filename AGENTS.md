# AGENTS

## Objetivo do repositorio
- Transformar dados publicos do Cartola FC em analises acessiveis e visualizacoes uteis.
- Construir uma base modular que permita experimentos com modelos estatisticos e aplicacoes em tempo real.

## Papeis de agentes

### Agent: Projetista de Dados
Responsavel por mapear as fontes oficiais, definir esquemas intermediarios e garantir qualidade e consistencia.
- Entregar documentacao de campos e dicionario de dados.
- Automatizar validacoes basicas (tipos, faixas permitidas, valores faltantes).

### Agent: Engenheiro de Pipelines
Cuida da ingestao, transformacao e persistencia dos dados.
- Criar conectores para os endpoints listados em `README.md`.
- Padronizar ingestao incremental e historico bruto.
- Integrar testes automatizados para cada pipeline critica.

### Agent: Analista ou Scientist
Explora os dados, gera metricas de performance e prototipos de modelos.
- Priorizar metricas de rodada, consistencia e scouting.
- Entregar notebooks ou scripts reprodutiveis com explicacao dos achados.

### Agent: Dev de Produto
Transforma analises em interfaces (dashboards, APIs internas ou bots).
- Reutilizar camadas de dados existentes.
- Garantir experiencia de desenvolvimento simples (CLI, docs, exemplos) para novos usuarios.

## Fluxo recomendado
1. Sincronizar com o estado atual (ver `README.md`).
2. Registrar objetivos e entregas no board do projeto.
3. Trabalhar em feature branches curtas com commits explicativos.
4. Solicitar revisao cruzada entre agentes antes de mesclar.

## Padroes de contribuicao
- Codigo em Python >= 3.11 quando possivel.
- Organizacao modular (`src/`, `notebooks/`, `docs/`).
- Tests via `pytest` com cobertura minima crescente (>95% nas features estaveis).
- Documentacao minima em cada entrega (README local ou docstring).

## Observabilidade e etica
- Tratar limites de uso das APIs publicas; respeitar intervalos entre requisicoes.
- Anonimizar dados sensiveis (ligas privadas, usuarios individuais) antes de uso publico.
- Logar falhas e compartilhar licoes aprendidas no diretorio `docs/retrospectives/`.
