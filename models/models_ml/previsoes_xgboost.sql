{{
    config(
        materialized='incremental',
        unique_key=['atleta_id', 'rodada_alvo'],
        incremental_strategy='merge'
    )
}}

-- Tabela populada pelo notebook 02_ml_xgboost.py via MLflow / PySpark.
-- Este modelo dbt expõe os resultados para consulta.

select
    atleta_id,
    rodada_alvo,
    pontuacao_prevista,
    rmse,
    mae,
    r2_score,
    coverage_score,
    modelo_versao,
    run_id_mlflow,
    _generated_at
from {{ source('models_ml', 'raw_previsoes_xgboost') }}

{% if is_incremental() %}
where rodada_alvo >= (select max(rodada_alvo) from {{ this }})
{% endif %}
