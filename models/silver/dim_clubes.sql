{{
    config(
        materialized='table',
        unique_key='clube_id'
    )
}}

with source as (
    select * from {{ ref('stg_clubes') }}
),

latest as (
    select *,
        row_number() over (partition by clube_id order by _ingestao_timestamp desc) as rn
    from source
),

final as (
    select
        clube_id,
        nome,
        abreviacao,
        slug,
        apelido,
        nome_fantasia,
        url_editoria,
        coalesce(escudo_60x60, escudo_45x45, escudo_30x30) as escudo_url,
        _ingestao_timestamp                                  as _updated_at
    from latest
    where rn = 1
)

select * from final
