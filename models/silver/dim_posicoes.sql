{{
    config(
        materialized='table',
        unique_key='posicao_id'
    )
}}

select
    posicao_id,
    nome,
    abreviacao
from {{ ref('seed_posicoes') }}
