with source as (
    select * from {{ source('bronze', 'raw_partidas') }}
)

select
    partida_id,
    rodada_id,
    clube_casa_id,
    clube_visitante_id,
    clube_casa_posicao,
    clube_visitante_posicao,
    placar_oficial_mandante,
    placar_oficial_visitante,
    partida_data,
    timestamp_partida,
    local               as estadio,
    valida,
    campeonato_id,
    aproveitamento_mandante,
    aproveitamento_visitante,
    _ingestao_timestamp,
    _batch_id
from source
