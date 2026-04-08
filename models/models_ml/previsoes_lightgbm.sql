{{
    config(
        materialized='incremental',
        unique_key=['atleta_id', 'rodada_alvo'],
        incremental_strategy='merge'
    )
}}

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
from {{ source('models_ml', 'raw_previsoes_lightgbm') }}

{% if is_incremental() %}
where rodada_alvo >= (select max(rodada_alvo) from {{ this }})
{% endif %}
