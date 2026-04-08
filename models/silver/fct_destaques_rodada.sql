{{
    config(
        materialized='incremental',
        unique_key='rodada_id',
        incremental_strategy='merge'
    )
}}

with source as (
    select * from {{ ref('stg_pos_rodada_destaques') }}
    {% if is_incremental() %}
    where rodada_id >= (select max(rodada_id) from {{ this }})
    {% endif %}
)

select
    rodada_id,
    mito_time_id,
    mito_nome,
    mito_nome_cartola,
    mito_slug,
    mito_clube_id,
    media_cartoletas,
    media_pontos,
    _ingestao_timestamp
from source
