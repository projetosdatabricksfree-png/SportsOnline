# CLAUDE.md — Cartola FC Analytics Platform

## Visão Geral

Plataforma de ELT em tempo real para dados do **Cartola FC** (Fantasy Game do Brasileirão). Arquitetura **Medallion (Bronze → Silver → Gold)** no Databricks Unity Catalog, transformações via **dbt**, e 3 modelos preditivos + meta-modelo de ensemble para prever desempenho de atletas.

**Stack:**
- Plataforma: Databricks (Unity Catalog)
- Transformação: dbt-databricks (SQL declarativo)
- Linguagem: Python (PySpark) + SQL
- API Source: `api.cartola.globo.com` (sem auth)
- Orquestração: Databricks Workflows + dbt
- IDE: VS Code + Claude Code

---

## Estrutura do Projeto

```
Sports_Online_Databricks/
├── models/
│   ├── bronze/          # stg_* — ingestão bruta (1:1 com APIs)
│   ├── silver/          # dim_* e fct_* — dados limpos e normalizados
│   ├── gold/            # métricas agregadas e feature store para ML
│   └── models_ml/       # previsões dos 3 modelos + previsao_final
├── macros/              # parse_scout_json, calcular_resultado, coverage_score
├── seeds/               # seed_posicoes, seed_pontuacao_scouts, seed_classicos
├── snapshots/           # snap_preco_atletas (SCD Type 2)
├── tests/
├── dbt_project.yml
└── PRD_Sports_Online.md # Requisitos completos com schemas, DDLs e arquitetura
```

---

## Unity Catalog — Estrutura

```
catalog: cartola_fc
├── schema: bronze    -- raw JSON → Delta (metadados: _raw_json, _ingestao_timestamp, _batch_id)
├── schema: silver    -- dados tipados, enriquecidos, com constraints
├── schema: gold      -- métricas acumuladas + feature_store_previsao (input dos modelos ML)
└── schema: models    -- saídas dos modelos preditivos e meta-modelo
```

---

## APIs da Cartola FC — Endpoints Sem Auth

| Endpoint | Frequência | Tabela Bronze |
|----------|-----------|---------------|
| `/mercado/status` | Mudança de estado | `bronze.raw_mercado_status` |
| `/atletas/mercado` | Por rodada (PRINCIPAL, 740+ atletas) | `bronze.raw_atletas_mercado` |
| `/atletas/pontuados` | Durante/após jogos | `bronze.raw_atletas_pontuados` |
| `/partidas` e `/partidas/{rodada}` | Por rodada | `bronze.raw_partidas` |
| `/clubes` | Início temporada | `bronze.raw_clubes` |
| `/rodadas` | Estático | `bronze.raw_rodadas` |
| `/pos-rodada/destaques` | Pós-rodada | `bronze.raw_pos_rodada_destaques` |
| `/ligas` | Dinâmico | `bronze.raw_ligas` |

**Base URL:** `https://api.cartola.globo.com`

---

## Modelos dbt por Camada

### Bronze (`stg_*`) — materialized: view
`stg_atletas_mercado`, `stg_atletas_pontuados`, `stg_partidas`, `stg_clubes`, `stg_rodadas`, `stg_mercado_status`, `stg_pos_rodada_destaques`, `stg_ligas`

### Silver (`dim_*`, `fct_*`) — materialized: table
- Dims: `dim_clubes`, `dim_posicoes`, `dim_atletas`, `dim_rodadas`
- Facts: `fct_atletas_rodada` (scout expandido, particionado por rodada_id), `fct_partidas`, `fct_mercado_status`, `fct_destaques_rodada`

### Gold — materialized: table/incremental
`metricas_atleta_acumulado`, `metricas_clube_rodada`, `tabela_brasileirao`, `feature_store_previsao` (particionado por rodada_alvo)

### Models ML
`previsoes_xgboost`, `previsoes_lightgbm`, `previsoes_poisson`, `previsao_final`

---

## Tabela de Scouts — Pontuação Cartola FC

| Scout | Descrição | Pontos |
|-------|-----------|--------|
| G | Gol | +8.0 |
| A | Assistência | +5.0 |
| SG | Saldo de Gol (não sofrer) | +5.0 |
| DE | Defesa (goleiro) | +3.0 |
| DP | Defesa de Pênalti | +7.0 |
| DS | Desarme | +1.5 |
| FT | Finalização na Trave | +3.0 |
| CA | Cartão Amarelo | -2.0 |
| CV | Cartão Vermelho | -5.0 |
| GC | Gol Contra | -5.0 |
| FC | Falta Cometida | -0.5 |

---

## Modelos Preditivos

```
gold.feature_store_previsao
        │
   ┌────┴────┬────────────┐
   │         │            │
XGBoost  LightGBM  Poisson-Bayesiano
   │         │            │
   └────┬────┴────────────┘
        │
   META-MODELO (Stacking por Coverage Score)
        │
   previsao_final
```

**Meta-modelo** seleciona dinamicamente o melhor preditor por **coverage score** (% previsões dentro do threshold por posição) usando janela deslizante das últimas 3 rodadas.

---

## Comandos dbt

```bash
# Ativar ambiente
source venv/bin/activate

# Desenvolvimento
dbt run                              # Todos os modelos
dbt run --select bronze              # Só camada bronze
dbt run --select +fct_atletas_rodada # Modelo + upstream
dbt run --full-refresh               # Rebuild incrementais

# Qualidade
dbt test                             # Todos os testes
dbt build                            # run + test em ordem DAG

# Debug
dbt compile --select <modelo>        # Compila sem executar
dbt debug                            # Testa conexão
dbt parse                            # Valida estrutura

# Docs
dbt docs generate && dbt docs serve
```

---

## Configuração Databricks

Perfil `databrickssports` em `~/.dbt/profiles.yml`:

```yaml
databrickssports:
  target: dev
  outputs:
    dev:
      type: databricks
      host: [workspace-url]
      http_path: [cluster-http-path]
      token: [PAT-token]
      catalog: cartola_fc
      schema: bronze
      threads: 4
```

**Nunca commitar `profiles.yml`** com credenciais.

---

## Convenções

- **Bronze** → prefixo `stg_`, `raw_` — dado bruto, sem transformação de negócio
- **Silver** → prefixo `dim_` (dimensões) ou `fct_` (fatos) — dados tipados com constraints
- **Gold** → sem prefixo padrão — métricas derivadas e feature store
- **Scouts** no bronze ficam em `scout_json` (STRING); Silver expande para colunas `scout_*`
- Metadados de ingestão: `_raw_json`, `_ingestao_timestamp`, `_batch_id`, `_rodada_captura`

---

## Skills Disponíveis

| Skill | Quando usar |
|-------|-------------|
| `databricks-core` | Autenticação, CLI, navegação de clusters |
| `databricks-pipelines` | Lakeflow / Delta Live Tables |
| `databricks-jobs` | Agendamento de workflows |
| `databricks-dabs` | Declarative Automation Bundles |
| `dbt-transformation-patterns` | Padrões de modelos, testes, incrementais |

> Consulte `PRD_Sports_Online.md` para schemas completos de APIs, DDLs completas das tabelas e detalhes dos algoritmos ML.
