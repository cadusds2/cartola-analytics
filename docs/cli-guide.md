# Guia de Coleta de Dados

Este documento descreve como preparar o ambiente, parametrizar variaveis e executar a CLI `cartola-fetch` para coletar payloads brutos dos endpoints do Cartola FC.

## Pre requisitos
- Python >= 3.11
- Poetry instalado (`poetry --version`), opcional por?m recomendado
- Dependencias do projeto instaladas (`poetry install`)

## Configuracao de ambiente
1. Copie `.env.example` para `.env`.
2. Ajuste os seguintes campos conforme o contexto:
   - `CARTOLA_TIMEOUT`: timeout em segundos por requisicao (10 por padrao).
   - `CARTOLA_MAX_RETRIES`: numero maximo de tentativas em falhas (3).
   - `CARTOLA_BACKOFF_FACTOR`: fator de exponenciacao para espera entre tentativas (0.5).
   - `CARTOLA_CACHE_TTL`: tempo em segundos para manter respostas em cache local (600).
   - `CARTOLA_CACHE_DIR`: diretorio de cache (`data/cache`). Use `none` para desativar.
   - `CARTOLA_RAW_DIR`: diretorio base para salvar payloads (`data/raw`).
   - `CARTOLA_LOG_LEVEL`: nivel minimo de log (`INFO`, `DEBUG`, etc.).
   - `CARTOLA_LOG_FILE`: caminho opcional para arquivo de log. Quando definido, logs sao gravados tanto em stdout quanto no arquivo.

## Executando a CLI
O entrypoint `cartola-fetch` e registrado via Poetry. Existem duas formas de invocar o comando:

```
poetry run cartola-fetch --list
cartola-fetch --list  # apos instala??o do pacote em um ambiente Poetry/pip
```

### Listar endpoints disponiveis
```
poetry run cartola-fetch --list
```
A sa?da mostra o nome interno usado pelo projeto. Endpoints que dependem de rodada exibem o sufixo `(rodada)`.

### Coletar endpoints especificos
```
poetry run cartola-fetch clubes mercado_status
```
Os arquivos sao salvos em `<CARTOLA_RAW_DIR>/<endpoint>/<timestamp>.json`. Use `--output` para direcionar a outro diretorio.

### Coletar todos os endpoints
```
poetry run cartola-fetch --all
```
Quando `--rodada` nao for informado, o comando busca automaticamente a lista de rodadas disponiveis via endpoint `rodadas` e coleta cada uma delas.
Se algum endpoint falhar (ex.: 404 ou payload invalido) a execucao segue adiante, o erro e reportado no final e o comando retorna status 1.
Para restringir a uma rodada especifica, informe o parametro:
```
poetry run cartola-fetch --all --rodada 5
```

### Opcoes adicionais
- `--use-cache`: reutiliza respostas armazenadas no cache local, respeitando `CARTOLA_CACHE_TTL`.
- `--output <path>`: sobrescreve `CARTOLA_RAW_DIR` apenas para a execucao atual.

## Estrutura de logs
- Logs sao sempre emitidos em JSON (stdout).
- Quando `CARTOLA_LOG_FILE` esta definido, um arquivo e criado com o mesmo formato JSON.
- Campos relevantes: `event`, `endpoint`, `rodada`, `path`, `error`.

## Fluxo interno
1. `cartola-fetch` carrega `CartolaSettings` (variaveis de ambiente).
2. `configure_logging_from_settings` inicia os handlers (stdout e opcionalmente arquivo).
3. `CartolaClient` reutiliza a configuracao, aplicando timeout, retries e cache.
4. `collect_endpoint_payload` resolve o caminho com timestamp UTC e grava o JSON bruto.

## Dicas e diagnostico
- Adicione `CARTOLA_LOG_LEVEL=DEBUG` para inspecionar requests, tentativas e cache.
- Utilize `rm -rf data/cache/*` para limpar o cache rapidamente entre execucoes.
- Combine com orquestradores (Prefect/Dagster) chamando o comando via shell ou importando `collect_endpoint_payload` diretamente.

---
Atualize este guia com novos endpoints, parametros ou flags sempre que alterar a CLI.
