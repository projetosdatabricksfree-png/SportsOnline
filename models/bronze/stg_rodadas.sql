with source as (
    select * from {{ source('bronze', 'raw_rodadas') }}
)

select
    rodada_id,
    nome_rodada,
    inicio,
    fim,
    _ingestao_timestamp,
    _batch_id
from source
