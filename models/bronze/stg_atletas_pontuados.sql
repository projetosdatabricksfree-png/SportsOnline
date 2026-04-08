with source as (
    select * from {{ source('bronze', 'raw_atletas_pontuados') }}
)

select
    atleta_id,
    apelido,
    clube_id,
    posicao_id,
    pontuacao,
    scout_json,
    entrou_em_campo,
    _rodada_id          as rodada_id,
    _ingestao_timestamp,
    _batch_id
from source
