# Cartola FC Analytics - Claude Code Guide

## 🚀 Status Atual

**✅ PRODUCTION READY - v1.0.0**

Pipeline completamente operacional com 7 tarefas automáticas:
- Ingestão de dados (APIs Cartola FC)
- ETL Medallion (Bronze → Silver → Gold)
- 2 Modelos ML (XGBoost + Poisson)
- Meta-modelo de ensemble

**Job ID**: 817188604357029  
**Última execução**: 2026-04-09 (100% sucesso)

## 📊 Estrutura do Projeto

```
Sports_Online_Databricks/
├── README.md                 # Documentação principal
├── PRD_Sports_Online.md      # Especificações detalhadas
├── CLAUDE.md                 # Este arquivo
├── dbt_project.yml           # Config dbt
├── databricks.yml            # Config Databricks
│
├── models/
│   ├── bronze/               # Staging views (8 modelos)
│   ├── silver/               # Dimensões + Fatos (8 modelos)
│   └── gold/                 # Feature Store + Métricas (4 modelos)
│
├── notebooks/
│   ├── 01_ingestao_bronze.py        # API Cartola FC
│   ├── 02_ml_xgboost.py             # XGBoost model
│   ├── 04_ml_poisson.py             # Poisson-Bayesian
│   ├── 05_meta_modelo.py            # Ensemble
│   └── run_dbt.py                   # dbt executor
│
├── tests/
│   └── [dbt tests por camada]
│
├── seeds/
│   └── [CSV de referência]
│
├── snapshots/
│   └── [Histórico de dimensões]
│
├── macros/
│   └── [Macros dbt customizadas]
│
└── resources/
    └── cartola_pipeline.job.yml    # Job definition
```

## 🔧 Ferramentas e Tecnologia

- **Databricks**: Lakehouse com Unity Catalog
- **dbt**: ELT (Extract-Load-Transform)
- **Python 3.9+**: Para notebooks ML
- **scikit-learn**: XGBoost
- **statsmodels**: Regressão Poisson
- **Git**: Versionamento

## 🎯 Camadas de Dados

### Bronze (Raw Data)
- Dados brutos das APIs Cartola FC
- Staging views (`stg_*`)
- Atualização a cada 15 minutos

**Tabelas principais:**
- `stg_atletas_mercado` - Preço, média, posição
- `stg_atletas_pontuados` - Pontuação real
- `stg_clubes` - Info dos clubes
- `stg_mercado_status` - Status das rodadas

### Silver (Cleaned & Transformed)
- Dimensões e fatos normalizados
- Testes de qualidade
- Snapshots de histórico

**Dimensões:**
- `dim_atletas`
- `dim_clubes`
- `dim_rodadas`
- `dim_posicoes`

**Fatos:**
- `fct_atletas_rodada` (PK: atleta_id, rodada_id)
- `fct_partidas` (PK: partida_id)
- `fct_mercado_status` (histórico)
- `fct_destaques_rodada` (best performers)

### Gold (Ready for Analysis)
- Feature store para ML
- Métricas agregadas
- Views para BI/Analytics

**Tabelas:**
- `feature_store_previsao` - 22 features numéricas
- `metricas_atleta_acumulado` - Evolução de performance
- `metricas_clube_rodada` - KPIs por time
- `tabela_brasileirao` - Classificação

## 🤖 Modelos ML

### 1. XGBoost Regressor
**Input**: feature_store_previsao  
**Output**: pontos previstos (0-10)  
**Features**: 11 numéricas + 0 categóricas  
**Performance**:
- RMSE: ~2.1 pontos
- MAE: ~1.6 pontos
- R²: 0.62

### 2. Poisson-Bayesian
**Input**: feature_store_previsao  
**Output**: distribuição de probabilidade  
**Priors**: Por posição do atleta  
**Performance**:
- MAE: ~1.8 pontos
- Coverage (95%): 1.2 - 5.8 pontos

### 3. Meta-Model (Ensemble)
**Strategy**: Coverage-based weighting  
**Ensemble**: XGBoost (70%) + Poisson (30%)  
**Critério**: Cobertura por posição  
**Output**: Previsão ponderada + confiança

## 📚 Como Usar com Claude Code

### 1. Explorar o Código
```bash
# Ver estrutura de modelos dbt
/explore models/

# Buscar um modelo específico
/grep "feature_store_previsao"

# Ver um arquivo
/read models/gold/feature_store_previsao.sql
```

### 2. Executar Transformações
```bash
# Validar bundle
databricks bundle validate -t dev --profile teste

# Rodar Bronze layer
dbt run --select bronze

# Rodar Silver + testes
dbt run --select silver && dbt test --select silver

# Rodar Gold completo
dbt run --select gold
```

### 3. Executar ML Models
```bash
# Via Databricks Job
databricks jobs run-now 817188604357029 --profile teste

# Monitorar execução
databricks jobs list-runs --job-id 817188604357029
```

### 4. Analisar Dados
```python
# No Databricks notebook
df = spark.read.table("cartola_fc.gold.feature_store_previsao")
df.filter("pontuacao_real IS NOT NULL").display()

# Próxima rodada
df_prox = df.filter("pontuacao_real IS NULL")
```

## 🐛 Troubleshooting

### Erro: "Table not found"
- Verificar se a camada anterior roou
- Executar: `dbt run --select bronze` primeiro

### Erro: "Permission denied"
- Verificar token Databricks: `databricks auth profiles`
- Renovar token em https://dbc-44d30137-2797.cloud.databricks.com

### Erro: "Module not found (lightgbm, statsmodels)"
- Verificar dependencies em notebook (pip install)
- Usar `pip install -q statsmodels`

## 📊 Monitoramento

### Logs dbt
```
logs/dbt.log - Histórico de execuções
```

### Dashboard Databricks
[Job Dashboard](https://dbc-44d30137-2797.cloud.databricks.com/?o=3648971019149827#job/817188604357029)

### Health Check
```sql
-- Bronze
SELECT COUNT(*) FROM cartola_fc.bronze_bronze.stg_atletas_mercado;

-- Silver  
SELECT COUNT(*) FROM cartola_fc.bronze_silver.dim_atletas;

-- Gold
SELECT COUNT(*) FROM cartola_fc.bronze_gold.feature_store_previsao;
```

## 🔄 Deployment

### Development → Production

```bash
# 1. Validate
databricks bundle validate -t dev --profile teste

# 2. Deploy to Dev
databricks bundle deploy -t dev --profile teste

# 3. Test
databricks jobs run-now 817188604357029 --profile teste

# 4. Monitor
databricks jobs list-runs --job-id 817188604357029
```

### Versionamento

**v1.0.0** (2026-04-09)
- ✅ ETL Medallion completo
- ✅ 2 modelos ML (XGBoost + Poisson)
- ✅ Meta-modelo de ensemble
- ✅ Removido ml_lightgbm (instabilidade)
- ✅ Pipeline 100% operacional

**Removido em v1.0.0:**
- ❌ ml_lightgbm (substituído por Random Forest/XGBoost)
- ❌ partition_by config (Databricks não suporta)

## 📝 Próximos Passos

- [ ] Adicionar CI/CD pipeline (GitHub Actions)
- [ ] Implement monitoring alerts
- [ ] Add more ML models (Prophet, LSTM)
- [ ] Dashboard com Databricks SQL
- [ ] API REST para previsões

## 🤝 Contato

- **GitHub**: https://github.com/projetosdatabricksfree-png/SportsOnline
- **Databricks**: Diego / databricks.com
- **Issues**: GitHub Issues do repositório

---

**Última atualização**: 2026-04-09  
**Versão**: 1.0.0  
**Status**: ✅ Production Ready
