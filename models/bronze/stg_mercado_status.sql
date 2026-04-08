with source as (
    select * from {{ source('bronze', 'raw_mercado_status') }}
)

select
    rodada_atual,
    status_mercado,
    temporada,
    game_over,
    times_escalados,
    mercado_pos_rodada,
    bola_rolando,
    nome_rodada,
    rodada_final,
    cartoleta_inicial,
    esquema_default_id,
    fechamento_dia,
    fechamento_mes,
    fechamento_ano,
    fechamento_hora,
    fechamento_minuto,
    fechamento_timestamp,
    _ingestao_timestamp,
    _batch_id
from source
