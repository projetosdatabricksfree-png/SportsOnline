with source as (
    select * from {{ source('bronze', 'raw_pos_rodada_destaques') }}
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
    _ingestao_timestamp,
    _batch_id
from source
