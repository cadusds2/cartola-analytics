# Integracao automatica de transformacoes pos-coleta

## Descricao
Padronizar o fluxo da CLI para disparar automaticamente as transformacoes disponiveis logo apos a coleta bem-sucedida de cada endpoint elegivel.

## Objetivo
Garantir que camadas `stage` e `processed` sejam atualizadas sem etapas manuais, mantendo consistencia entre payloads brutos e datasets tratados.

## Atividades
- [x] rodadas - `transform_rodadas` executada apos coleta via CLI (implementado)
- [x] mercado_status - acionar `transform_mercado_status` quando o endpoint for coletado
- [ ] partidas - definir pipeline de transformacao e integrar a CLI
- [ ] clubes - definir pipeline de transformacao e integrar a CLI
- [ ] atletas_mercado - definir pipeline de transformacao e integrar a CLI
- [ ] atletas_pontuados - definir pipeline de transformacao e integrar a CLI
- [ ] pos_rodada_destaques - definir pipeline de transformacao e integrar a CLI