# Design Spec — Plataforma Cartola FC Analytics
**Data:** 2026-04-08
**Status:** Aprovado

---

## Visão Geral

Plataforma de ELT em tempo real para dados do Cartola FC (Fantasy Game do Brasileirão Série A). Coleta dados das APIs públicas a cada 15 minutos, transforma via arquitetura Medallion (Bronze → Silver → Gold) no Databricks Unity Catalog com dbt, e alimenta 3 modelos preditivos + meta-modelo de ensemble para prever pontuação de atletas.

---

## Decisões de Arquitetura

| Decisão | Escolha | Motivo |
|---------|---------|--------|
| Catalog | Unity Catalog `cartola_fc` | Governança, controle de acesso, lineage |
| Ingestão | DABs (`.py` notebooks versionados) | Tudo no git, deploy unificado |
| ML Lifecycle | MLflow + DABs | Tracking de experimentos, model registry |
| Documentação | Markdown não-técnico | Acessível, convertível para PDF/HTML |
| Organização | Monorepo (dbt + DABs) | Um único deploy, histórico unificado |

---

## Estrutura do Repositório

```
Sports_Online_Databricks/
├── databricks.yml                        ← Bundle DABs central
├── dbt_project.yml                       ← Atualizado: catalog cartola_fc
├── packages.yml                          ← dbt-utils
│
├── models/
│   ├── bronze/
│   │   ├── _bronze_sources.yml
│   │   ├── stg_atletas_mercado.sql
│   │   ├── stg_atletas_pontuados.sql
│   │   ├── stg_partidas.sql
│   │   ├── stg_clubes.sql
│   │   ├── stg_rodadas.sql
│   │   ├── stg_mercado_status.sql
│   │   ├── stg_pos_rodada_destaques.sql
│   │   └── stg_ligas.sql
│   ├── silver/
│   │   ├── _silver_schema.yml
│   │   ├── dim_clubes.sql
│   │   ├── dim_posicoes.sql
│   │   ├── dim_atletas.sql
│   │   ├── dim_rodadas.sql
│   │   ├── fct_atletas_rodada.sql
│   │   ├── fct_partidas.sql
│   │   ├── fct_mercado_status.sql
│   │   └── fct_destaques_rodada.sql
│   ├── gold/
│   │   ├── _gold_schema.yml
│   │   ├── metricas_atleta_acumulado.sql
│   │   ├── metricas_clube_rodada.sql
│   │   ├── tabela_brasileirao.sql
│   │   └── feature_store_previsao.sql
│   └── models_ml/
│       ├── _models_schema.yml
│       ├── previsoes_xgboost.sql
│       ├── previsoes_lightgbm.sql
│       ├── previsoes_poisson.sql
│       └── previsao_final.sql
│
├── notebooks/
│   ├── 00_setup_unity_catalog.py         ← Cria catalog + schemas (roda uma vez)
│   ├── 01_ingestao_bronze.py             ← Chama APIs, upsert nas tabelas bronze
│   ├── 02_ml_xgboost.py                  ← XGBoost + Hyperopt + MLflow
│   ├── 03_ml_lightgbm.py                 ← LightGBM + SHAP + MLflow
│   ├── 04_ml_poisson.py                  ← Poisson-Bayesiano + MLflow
│   └── 05_meta_modelo.py                 ← Coverage scoring + previsão final
│
├── macros/
│   ├── parse_scout_json.sql
│   ├── calcular_resultado.sql
│   ├── calcular_tendencia.sql
│   └── coverage_score.sql
│
├── seeds/
│   ├── seed_posicoes.csv
│   ├── seed_pontuacao_scouts.csv
│   ├── seed_classicos.csv
│   └── seed_status_atleta.csv
│
├── snapshots/
│   └── snap_preco_atletas.sql            ← SCD Type 2 do preço
│
├── tests/
│   ├── assert_atleta_unicidade.sql
│   ├── assert_rodada_range.sql
│   └── assert_placar_nao_negativo.sql
│
├── resources/
│   └── cartola_pipeline.job.yml          ← Workflow com 8 tarefas encadeadas
│
└── docs/
    ├── superpowers/specs/                ← Este arquivo
    └── cartola_fc_para_todos.md          ← Documentação não-técnica
```

---

## Camada Bronze

**Notebook `01_ingestao_bronze.py`** — executa a cada 15 min via Workflow.

- Chama 8 endpoints públicos de `api.cartola.globo.com`
- Upsert (MERGE) nas tabelas Delta `cartola_fc.bronze.*`
- Metadados adicionados: `_raw_json`, `_ingestao_timestamp`, `_batch_id`, `_rodada_captura`

**Tabelas Bronze:**

| Tabela | Fonte API | Partição |
|--------|-----------|----------|
| `raw_atletas_mercado` | `/atletas/mercado` | `_rodada_captura` |
| `raw_atletas_pontuados` | `/atletas/pontuados` | `_rodada_id` |
| `raw_partidas` | `/partidas` | `rodada_id` |
| `raw_clubes` | `/clubes` | — |
| `raw_rodadas` | `/rodadas` | — |
| `raw_mercado_status` | `/mercado/status` | — |
| `raw_pos_rodada_destaques` | `/pos-rodada/destaques` | — |
| `raw_ligas` | `/ligas` | — |

**Modelos dbt Bronze:** `stg_*` como `view`, apenas renomeiam colunas e definem sources. Sem transformação de negócio.

**Seeds (carga única):** `seed_posicoes`, `seed_pontuacao_scouts`, `seed_classicos`, `seed_status_atleta`.

---

## Camada Silver

**Dimensões** (`dim_*`, materialized: `table`):

| Modelo | Fonte principal | Destaque |
|--------|----------------|---------|
| `dim_clubes` | `stg_clubes` | 20 clubes com nome, escudo, slug |
| `dim_posicoes` | `seed_posicoes` | GOL/LAT/ZAG/MEI/ATA/TEC |
| `dim_atletas` | `stg_atletas_mercado` | Cadastro + `status_descricao` enriquecido |
| `dim_rodadas` | `stg_rodadas` + `stg_mercado_status` | Calendário com datas e temporada |

**Fatos** (`fct_*`, materialized: `incremental`, strategy: `merge`):

| Modelo | Destaque |
|--------|---------|
| `fct_atletas_rodada` | Scout JSON → 20 colunas `scout_*` via macro `parse_scout_json`. Particionado por `rodada_id` |
| `fct_partidas` | Placares, V/E/D, total gols, aproveitamento últimos 5 jogos |
| `fct_mercado_status` | Histórico de estado do mercado por rodada |
| `fct_destaques_rodada` | Mito da rodada, médias gerais |

**Testes:** `unique` + `not_null` em PKs, `relationships` fatos→dims, `accepted_values` para `status_id` e `posicao_id`.

---

## Camada Gold

| Modelo | Materialization | O que entrega |
|--------|----------------|--------------|
| `metricas_atleta_acumulado` | `table` | Média, desvio, max/min, tendência últimas 5, `consistencia_score`, `preco_por_ponto` |
| `metricas_clube_rodada` | `incremental` | Gols, resultado, sequências, posição na tabela |
| `tabela_brasileirao` | `incremental` | Classificação calculada: pontos, saldo, aproveitamento |
| `feature_store_previsao` | `incremental` (por `rodada_alvo`) | Features atleta + clube + adversário + contexto. `pontuacao_real` é `NULL` quando criado (pré-rodada) e atualizado via merge com `stg_atletas_pontuados` após a rodada encerrar |

**Snapshot:** `snap_preco_atletas` — SCD Type 2, grava histórico de valorização a cada rodada.

---

## Modelos de ML

### Modelo 1 — XGBoost (`02_ml_xgboost.py`)
- Input: `gold.feature_store_previsao` (apenas rodadas com `pontuacao_real`)
- Validação: `TimeSeriesSplit` 5 folds
- Otimização: Hyperopt
- Métricas: RMSE, MAE, R², Coverage Score
- Output: `models.previsoes_xgboost` + experimento MLflow

### Modelo 2 — LightGBM (`03_ml_lightgbm.py`)
- Igual ao XGBoost com leaf-wise growth
- Adiciona SHAP values para explicabilidade
- Features categóricas nativas (`posicao_id`, `mando_campo`)
- Output: `models.previsoes_lightgbm` + experimento MLflow

### Modelo 3 — Poisson-Bayesiano (`04_ml_poisson.py`)
- GLM Poisson via statsmodels
- Priors informativos por posição
- Produz intervalo de credibilidade 80%
- Output: `models.previsoes_poisson` + experimento MLflow

### Meta-modelo — Coverage Scoring (`05_meta_modelo.py`)
- Avalia coverage score de cada modelo nas últimas 3 rodadas

**Thresholds por posição:**
| Posição | Threshold |
|---------|-----------|
| Goleiro / Técnico | ±2.0 pts |
| Lateral / Zagueiro | ±2.5 pts |
| Meia | ±3.0 pts |
| Atacante | ±3.5 pts |

- Se coverage > 70% isolado → usa só aquele modelo
- Senão → média ponderada pelos pesos de cobertura
- Seleção pode variar por posição
- Output: `models.previsao_final` + pesos registrados no MLflow

---

## Orquestração — Databricks Workflow

**Arquivo:** `resources/cartola_pipeline.job.yml`

```
[01_ingestao_bronze] → [02_dbt_bronze] → [03_dbt_silver]
                                                 ↓
                                         [04_dbt_gold]
                                                 ↓
                              ┌──────────────────┼──────────────────┐
                         [05_xgboost]      [06_lightgbm]      [07_poisson]
                              └──────────────────┼──────────────────┘
                                                 ↓
                                         [08_meta_modelo]
```

| Tarefa | Tipo | Depende de |
|--------|------|-----------|
| `01_ingestao_bronze` | Notebook Python | — |
| `02_dbt_bronze` | dbt task | `01` |
| `03_dbt_silver` | dbt task | `02` |
| `04_dbt_gold` | dbt task (`dbt build`) | `03` |
| `05_xgboost` | Notebook Python | `04` |
| `06_lightgbm` | Notebook Python | `04` |
| `07_poisson` | Notebook Python | `04` |
| `08_meta_modelo` | Notebook Python | `05`, `06`, `07` |

**Schedule:** A cada 15 minutos, sempre. O notebook `01_ingestao_bronze.py` verifica internamente `bola_rolando` e `status_mercado` para decidir quais endpoints chamar.
**Deploy:** `databricks bundle deploy && databricks bundle run cartola_pipeline`

---

## Documentação Não-Técnica

**Arquivo:** `docs/cartola_fc_para_todos.md`

Seções:
1. O que é esse projeto?
2. De onde vêm os dados?
3. O que fazemos com os dados? (Coleta → Limpeza → Análise)
4. Como o sistema prevê pontuações? (analogia dos 3 especialistas + árbitro)
5. Como saber quem escalar?
6. Diagrama visual do fluxo (Mermaid)
7. Glossário de 10 termos em linguagem simples

---

## Ordem de Implementação

1. `dbt_project.yml` + `packages.yml` + `databricks.yml` (infra base)
2. Seeds CSV
3. Macros
4. `00_setup_unity_catalog.py` + `01_ingestao_bronze.py`
5. Models Bronze (`stg_*` + `_bronze_sources.yml`)
6. Models Silver (`dim_*` + `fct_*`)
7. Models Gold + Snapshot
8. Models ML (SQL das tabelas de resultado)
9. Notebooks ML (`02` a `05`) + MLflow
10. `resources/cartola_pipeline.job.yml`
11. Tests customizados
12. `docs/cartola_fc_para_todos.md`
