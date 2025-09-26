# Padrão de Transformação Automática pós-Coleta

Este padrão garante que, sempre que a CLI `cartola-fetch` coleta payloads de um endpoint com transformação disponível, a camada tratada correspondente seja atualizada automaticamente na sequência.

## Quando aplicar
- O endpoint possui pipeline de transformação em `src/cartola_analytics/pipelines/*_transform.py` com contrato de schema YAML em `docs/schemas/`.
- A transformação não depende de parâmetros adicionais além do diretório de dados brutos.
- O tempo de execução agregado é aceitável para o fluxo interativo da CLI.

## Fluxo recomendado
1. Coletar o endpoint via `collect_endpoint_payload`, respeitando diretório `--output` quando fornecido.
2. Registrar o endpoint na lista de coletas bem-sucedidas.
3. Após o loop principal, verificar se o endpoint integrado foi coletado com sucesso.
4. Invocar a transformação correspondente (ex.: `transform_rodadas(raw_root=<dir>)`).
5. Logar resultado positivo (`cli_transform_<endpoint>`) com caminhos `stage` e `processed`.
6. Capturar exceções, logar `cli_transform_<endpoint>_failed` e adicionar erro à lista de falhas para que a CLI retorne status 1.

## Boas práticas
- Permitir que a transformação receba o diretório de dados brutos (`raw_root`) para suportar `--output` customizado.
- Garantir que o schema YAML esteja atualizado com `metadata.lineage` refletindo a etapa de transformação.
- Cobrir o fluxo com testes unitários que validem: disparo da transformação, propagação de erro e respeito ao diretório customizado.
- Manter logs em JSON com campos `event`, `raw_root`, `stage_path`, `processed_path`, `rows_stage`, `rows_processed`.

## Próximos passos
- Mapear os endpoints com pipelines existentes ou planejados.
- Criar tarefa única acompanhando a adoção deste padrão por endpoint (ver docs/issues).
- Reavaliar custo/benefício ao adicionar novos pipelines, principalmente para endpoints volumosos.
