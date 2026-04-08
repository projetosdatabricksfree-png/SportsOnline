{{
    config(
        materialized='incremental',
        unique_key=['clube_id', 'rodada_id'],
        incremental_strategy='merge',
        partition_by={
            'field': 'rodada_id',
            'data_type': 'int'
        }
    )
}}

with partidas as (
    select * from {{ ref('fct_partidas') }}
    where placar_mandante is not null
      and placar_visitante is not null
    {% if is_incremental() %}
    and rodada_id >= (select max(rodada_id) - 1 from {{ this }})
    {% endif %}
),

-- Perspectiva do mandante
mandante as (
    select
        clube_casa_id                as clube_id,
        rodada_id,
        placar_mandante              as gols_pro,
        placar_visitante             as gols_contra,
        case resultado_mandante
            when 'V' then 3
            when 'E' then 1
            else 0
        end                          as pontos,
        case when resultado_mandante = 'V' then 1 else 0 end as vitorias,
        case when resultado_mandante = 'E' then 1 else 0 end as empates,
        case when resultado_mandante = 'D' then 1 else 0 end as derrotas
    from partidas
),

-- Perspectiva do visitante
visitante as (
    select
        clube_visitante_id           as clube_id,
        rodada_id,
        placar_visitante             as gols_pro,
        placar_mandante              as gols_contra,
        case resultado_visitante
            when 'V' then 3
            when 'E' then 1
            else 0
        end                          as pontos,
        case when resultado_visitante = 'V' then 1 else 0 end as vitorias,
        case when resultado_visitante = 'E' then 1 else 0 end as empates,
        case when resultado_visitante = 'D' then 1 else 0 end as derrotas
    from partidas
),

todos as (
    select * from mandante
    union all
    select * from visitante
),

acumulado as (
    select
        clube_id,
        rodada_id,
        sum(pontos)         as pontos,
        count(*)            as jogos,
        sum(vitorias)       as vitorias,
        sum(empates)        as empates,
        sum(derrotas)       as derrotas,
        sum(gols_pro)       as gols_pro,
        sum(gols_contra)    as gols_contra,
        sum(gols_pro) - sum(gols_contra) as saldo_gols
    from todos
    group by clube_id, rodada_id
),

final as (
    select
        clube_id,
        rodada_id,
        rank() over (partition by rodada_id order by pontos desc, saldo_gols desc, gols_pro desc) as posicao,
        pontos,
        jogos,
        vitorias,
        empates,
        derrotas,
        gols_pro,
        gols_contra,
        saldo_gols,
        round(pontos / (jogos * 3.0) * 100, 1) as aproveitamento_percentual,
        current_timestamp()                     as _updated_at
    from acumulado
)

select * from final
