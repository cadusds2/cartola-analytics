# Data Dictionary

Generated automatically from docs/schemas.

## atletas_mercado
Informacoes de atletas em mercado, com scouts e precos vigentes.
- Dataset: `data/processed/atletas/atletas_mercado.parquet`
- Stage output: `data/stage/atletas_mercado/{run_timestamp}.parquet`
- Primary key: `atleta_id`
- Owner: `dados-cartola`
- Lineage: `cartola-fetch -> collect_endpoint_payload -> stage_atletas_mercado -> transform_atletas_mercado`
- Updated at: 2025-09-26T14:00:00Z

| Field | Type | Required | Description | Enum |
| --- | --- | --- | --- | --- |
| atleta_id | int | yes | Identificador unico do atleta. |  |
| rodada_id | int | no | Rodada de referencia do status do atleta. |  |
| clube_id | int | yes | Identificador do clube atual. |  |
| posicao_id | int | yes | Identificador da posicao do atleta. |  |
| status_id | int | yes | Identificador do status atual (provavel, duvida, etc.). |  |
| status_nome | string | no | Descricao do status do atleta. |  |
| clube_nome | string | no | Nome fantasia do clube associado. |  |
| posicao_nome | string | no | Nome textual da posicao. |  |
| nome | string | yes | Nome completo do atleta. |  |
| apelido | string | yes | Apelido utilizado no Cartola. |  |
| apelido_abreviado | string | no | Apelido abreviado exibido na interface. |  |
| slug | string | no | Slug textual do atleta. |  |
| foto | string | no | URL da foto/figurinha do atleta. |  |
| preco_num | float | yes | Preco atual do atleta em cartoletas. |  |
| media_num | float | yes | Media de pontos do atleta na temporada. |  |
| variacao_num | float | yes | Variacao de preco em relacao a rodada anterior. |  |
| pontos_num | float | no | Pontuacao na rodada anterior. |  |
| jogos_num | int | yes | Numero de jogos disputados na temporada. |  |
| entrou_em_campo | bool | no | Indica se o atleta atuou na rodada anterior. |  |
| scout_ca | int | no | Cartoes amarelos acumulados na rodada anterior (scout). |  |
| scout_cv | int | no | Cartoes vermelhos acumulados na rodada anterior (scout). |  |
| scout_fs | int | no | Faltas sofridas acumuladas na rodada anterior (scout). |  |
| scout_gc | int | no | Gols contra (scout). |  |
| scout_g | int | no | Gols marcados (scout). |  |
| scout_a | int | no | Assistencias (scout). |  |
| scout_fd | int | no | Finalizacoes na trave/defendidas (scout). |  |
| scout_ff | int | no | Finalizacoes para fora (scout). |  |
| scout_ft | int | no | Finalizacoes no alvo (scout). |  |
| scout_sg | int | no | Jogos sem sofrer gols (scout). |  |
| scout_pp | int | no | Penaltis perdidos (scout). |  |
| scout_dp | int | no | Defesas de penalti (scout). |  |
| scout_d | int | no | Defesas (goleiros). |  |
| scout_ds | int | no | Desarmes realizados. |  |
| scout_fc | int | no | Faltas cometidas. |  |
| scout_i | int | no | Impedimentos. |  |
| scout_pi | int | no | Penaltis inválidos/induzidos (quando presente). |  |
| timestamp_coleta | timestamp | yes | Momento UTC da coleta do payload. |  |

## atletas_pontuados
Pontuacao consolidada por atleta para a rodada atual.
- Dataset: `data/processed/atletas/atletas_pontuados.parquet`
- Stage output: `data/stage/atletas_pontuados/{run_timestamp}.parquet`
- Primary key: `atleta_id`
- Owner: `dados-cartola`
- Lineage: `cartola-fetch -> collect_endpoint_payload -> stage_atletas_pontuados -> transform_atletas_pontuados`
- Updated at: 2025-09-26T14:45:00Z

| Field | Type | Required | Description | Enum |
| --- | --- | --- | --- | --- |
| rodada | int | yes | Numero da rodada em que a pontuacao foi gerada. |  |
| atleta_id | int | yes | Identificador unico do atleta. |  |
| clube_id | int | no | Clube do atleta na rodada. |  |
| posicao_id | int | no | Posicao do atleta na rodada. |  |
| pontuacao | float | yes | Pontuacao final do atleta na rodada. |  |
| entrou_em_campo | bool | no | Indica se o atleta atuou na rodada. |  |
| apelido | string | no | Apelido ou nome curto do atleta. |  |
| slug | string | no | Slug textual do atleta. |  |
| scout_ca | int | no | Cartoes amarelos (scout). |  |
| scout_cv | int | no | Cartoes vermelhos (scout). |  |
| scout_fs | int | no | Faltas sofridas (scout). |  |
| scout_fc | int | no | Faltas cometidas (scout). |  |
| scout_ds | int | no | Desarmes (scout). |  |
| scout_g | int | no | Gols marcados (scout). |  |
| scout_a | int | no | Assistencias (scout). |  |
| scout_fd | int | no | Finalizacoes defendidas (scout). |  |
| scout_ff | int | no | Finalizacoes para fora (scout). |  |
| scout_ft | int | no | Finalizacoes no alvo (scout). |  |
| scout_pp | int | no | Penaltis perdidos (scout). |  |
| scout_dp | int | no | Defesas de penalti (scout). |  |
| scout_sg | int | no | Jogos sem sofrer gols (scout). |  |
| scout_gc | int | no | Gols contra (scout). |  |
| scout_i | int | no | Impedimentos (scout). |  |
| timestamp_coleta | timestamp | yes | Momento UTC da coleta do payload. |  |

## clubes
Cadastro de clubes com metadados e URLs de escudo.
- Dataset: `data/processed/clubes/clubes.parquet`
- Stage output: `data/stage/clubes/{run_timestamp}.parquet`
- Primary key: `clube_id`
- Owner: `dados-cartola`
- Lineage: `cartola-fetch -> collect_endpoint_payload -> stage_clubes -> transform_clubes`
- Updated at: 2025-09-26T13:20:00Z

| Field | Type | Required | Description | Enum |
| --- | --- | --- | --- | --- |
| clube_id | int | yes | Identificador unico do clube. |  |
| nome | string | yes | Nome abreviado do clube conforme Cartola. |  |
| nome_fantasia | string | no | Nome completo/fantasia do clube. |  |
| apelido | string | no | Apelido popular do clube. |  |
| abreviacao | string | no | Sigla de tres letras utilizada pelo Cartola. |  |
| slug | string | no | Slug textual utilizado em URLs internas. |  |
| escudo_30x30 | string | no | URL do escudo (30x30). |  |
| escudo_45x45 | string | no | URL do escudo (45x45). |  |
| escudo_60x60 | string | no | URL do escudo (60x60). |  |
| url_editoria | string | no | URL oficial do clube no ge.globo.com. |  |
| timestamp_coleta | timestamp | yes | Momento UTC da coleta do payload. |  |

## mercado_status
Status geral do mercado Cartola FC por temporada.
- Dataset: `data/processed/mercado/mercado_status.parquet`
- Stage output: `data/stage/mercado_status/{run_timestamp}.parquet`
- Primary key: `temporada`
- Owner: `dados-cartola`
- Lineage: `cartola-fetch -> collect_endpoint_payload -> stage_mercado_status -> transform_mercado_status`
- Updated at: 2025-09-25T03:54:00Z
- Relationships:
  - field=rodada_atual; dataset=data/processed/rodadas/rodadas.parquet (rodada_id); type=foreign_key

| Field | Type | Required | Description | Enum |
| --- | --- | --- | --- | --- |
| temporada | int | yes | Identificador da temporada (ano base). |  |
| rodada_atual | int | yes | Numero da rodada corrente. |  |
| rodada_final | int | yes | Numero da rodada final da temporada. |  |
| status_mercado | string | yes | Status textual (ABERTO, FECHADO, etc.). | ABERTO, FECHADO, EM_MANUTENCAO |
| mercado_pos_rodada | bool | yes | Indica se o mercado esta em periodo pos-rodada. |  |
| bola_rolando | bool | yes | Indica se ha partidas em andamento. |  |
| timestamp_fechamento | timestamp | yes | Timestamp UTC do fechamento do mercado atual. |  |
| timestamp_coleta | timestamp | yes | Momento UTC da coleta do payload. |  |

## partidas
Lista de partidas da rodada corrente com metadados de transmissao e desempenho recente.
- Dataset: `data/processed/partidas/partidas.parquet`
- Stage output: `data/stage/partidas/{run_timestamp}.parquet`
- Primary key: `partida_id`
- Owner: `dados-cartola`
- Lineage: `cartola-fetch -> collect_endpoint_payload -> stage_partidas -> transform_partidas`
- Updated at: 2025-09-26T12:30:00Z

| Field | Type | Required | Description | Enum |
| --- | --- | --- | --- | --- |
| rodada | int | yes | Numero da rodada referente ao conjunto de partidas. |  |
| partida_id | int | yes | Identificador unico da partida. |  |
| campeonato_id | int | yes | Identificador do campeonato na API Cartola. |  |
| partida_data | timestamp | yes | Data e hora programada para o inicio da partida (UTC). |  |
| timestamp_partida | timestamp | no | Timestamp UNIX convertido para datetime UTC. |  |
| timestamp_coleta | timestamp | yes | Momento UTC da coleta do payload. |  |
| clube_casa_id | int | yes | Identificador do clube mandante. |  |
| clube_casa_posicao | int | no | Posicao do clube mandante na tabela. |  |
| clube_visitante_id | int | yes | Identificador do clube visitante. |  |
| clube_visitante_posicao | int | no | Posicao do clube visitante na tabela. |  |
| placar_oficial_mandante | int | no | Placar oficial do mandante quando disponivel. |  |
| placar_oficial_visitante | int | no | Placar oficial do visitante quando disponivel. |  |
| aproveitamento_mandante | string | no | Sequencia de resultados recentes do mandante. |  |
| aproveitamento_visitante | string | no | Sequencia de resultados recentes do visitante. |  |
| valida | bool | yes | Indica se a partida e valida para pontuacao. |  |
| local | string | no | Descricao do estadio ou local da partida. |  |
| transmissao_label | string | no | Label informativo da transmissao. |  |
| transmissao_url | string | no | URL da transmissao/divulgacao associada. |  |
| status_transmissao_tr | string | no | Status textual da transmissao tempo real. |  |
| status_cronometro_tr | string | no | Status do cronometro em tempo real. |  |
| periodo_tr | string | no | Periodo exibido no tempo real (ex.: 1T, 2T). |  |
| inicio_cronometro_tr | string | no | Horario exibido para inicio de cronometro no tempo real. |  |

## pos_rodada_destaques
Destaque da rodada (mito) e estatisticas agregadas de media de pontos/cartoletas.
- Dataset: `data/processed/rodadas/pos_rodada_destaques.parquet`
- Stage output: `data/stage/pos_rodada_destaques/{run_timestamp}.parquet`
- Primary key: `rodada`
- Owner: `dados-cartola`
- Lineage: `cartola-fetch -> collect_endpoint_payload -> stage_pos_rodada_destaques -> transform_pos_rodada_destaques`
- Updated at: 2025-09-26T15:30:00Z

| Field | Type | Required | Description | Enum |
| --- | --- | --- | --- | --- |
| rodada | int | yes | Numero da rodada referente ao mito. |  |
| time_id | int | yes | Identificador do time vencedor da rodada. |  |
| nome_time | string | yes | Nome do time (cartola). |  |
| nome_cartola | string | yes | Nome do cartola (manager). |  |
| slug | string | no | Slug textual do time/cartola. |  |
| clube_id | int | no | Clube associado ao mito. |  |
| media_pontos | float | yes | Media de pontos da rodada. |  |
| media_cartoletas | float | yes | Media de cartoletas utilizadas na rodada. |  |
| tipo_escudo | int | no | Tipo de escudo configurado pelo time. |  |
| tipo_camisa | int | no | Tipo de camisa configurado pelo time. |  |
| esquema_id | int | no | Esquema tatico utilizado. |  |
| url_escudo_svg | string | no | URL SVG do escudo do time. |  |
| url_camisa_svg | string | no | URL SVG da camisa do time. |  |
| assinante | bool | no | Indica se o cartola é assinante. |  |
| timestamp_coleta | timestamp | yes | Momento UTC da coleta do payload. |  |

## rodadas
Janela de inicio e fim de cada rodada da temporada.
- Dataset: `data/processed/rodadas/rodadas.parquet`
- Stage output: `data/stage/rodadas/{run_timestamp}.parquet`
- Primary key: `rodada_id`
- Owner: `dados-cartola`
- Lineage: `cartola-fetch -> collect_endpoint_payload -> stage_rodadas -> transform_rodadas`
- Updated at: 2025-09-25T05:30:00Z

| Field | Type | Required | Description | Enum |
| --- | --- | --- | --- | --- |
| rodada_id | int | yes | Identificador sequencial da rodada. |  |
| nome_rodada | string | yes | Nome descritivo da rodada. |  |
| inicio | timestamp | yes | Data/hora de abertura da rodada. |  |
| fim | timestamp | yes | Data/hora de fechamento da rodada. |  |
| timestamp_coleta | timestamp | yes | Momento UTC da coleta do payload. |  |

