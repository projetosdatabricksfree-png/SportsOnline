{{
    config(
        materialized='table',
        unique_key='atleta_id'
    )
}}

with atletas as (
    select * from {{ ref('stg_atletas_mercado') }}
),

status_ref as (
    select * from {{ ref('seed_status_atleta') }}
),

latest as (
    select *,
        row_number() over (partition by atleta_id order by _ingestao_timestamp desc) as rn
    from atletas
),

final as (
    select
        a.atleta_id,
        a.nome,
        a.apelido,
        a.apelido_abreviado,
        a.slug,
        a.foto_url,
        a.posicao_id,
        a.clube_id,
        a.status_id,
        coalesce(s.descricao, 'Desconhecido') as status_descricao,
        a._ingestao_timestamp                  as _updated_at
    from latest a
    left join status_ref s on a.status_id = s.status_id
    where a.rn = 1
)

select * from final
