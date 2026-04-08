{{
    config(
        materialized='incremental',
        unique_key=['rodada_atual', '_ingestao_timestamp'],
        incremental_strategy='merge'
    )
}}

with source as (
    select * from {{ ref('stg_mercado_status') }}
    {% if is_incremental() %}
    where _ingestao_timestamp > (select max(_ingestao_timestamp) from {{ this }})
    {% endif %}
),

final as (
    select
        rodada_atual,
        status_mercado,
        case status_mercado
            when 1 then 'Aberto'
            when 2 then 'Fechado'
            when 4 then 'Em Manutenção'
            when 6 then 'Fim de Rodada'
            else   'Desconhecido'
        end                                    as status_descricao,
        temporada,
        game_over,
        times_escalados,
        bola_rolando,
        mercado_pos_rodada,
        to_timestamp(fechamento_timestamp)     as fechamento_datetime,
        _ingestao_timestamp                    as captura_timestamp
    from source
)

select * from final
