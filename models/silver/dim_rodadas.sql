{{
    config(
        materialized='table',
        unique_key='rodada_id'
    )
}}

with rodadas as (
    select * from {{ ref('stg_rodadas') }}
),

status as (
    select
        rodada_atual,
        temporada
    from {{ ref('stg_mercado_status') }}
    qualify row_number() over (partition by rodada_atual order by _ingestao_timestamp desc) = 1
),

final as (
    select
        r.rodada_id,
        r.nome_rodada,
        cast(r.inicio as timestamp) as data_inicio,
        cast(r.fim    as timestamp) as data_fim,
        coalesce(s.temporada, year(cast(r.inicio as timestamp))) as temporada,
        r._ingestao_timestamp       as _updated_at
    from rodadas r
    left join status s on r.rodada_id = s.rodada_atual
)

select * from final
