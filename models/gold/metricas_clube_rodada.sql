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

with atletas_rodada as (
    select * from {{ ref('fct_atletas_rodada') }}
    {% if is_incremental() %}
    where rodada_id >= (select max(rodada_id) - 1 from {{ this }})
    {% endif %}
),

partidas as (
    select * from {{ ref('fct_partidas') }}
    {% if is_incremental() %}
    where rodada_id >= (select max(rodada_id) - 1 from {{ this }})
    {% endif %}
),

tabela as (
    select * from {{ ref('tabela_brasileirao') }}
),

-- Métricas Cartola por clube por rodada
cartola_metrics as (
    select
        clube_id,
        rodada_id,
        sum(pontos_num)             as total_pontos_cartola,
        avg(pontos_num)             as media_pontos_cartola,
        count(distinct atleta_id)   as atletas_pontuaram
    from atletas_rodada
    where entrou_em_campo = true
    group by clube_id, rodada_id
),

-- Resultado real do clube na rodada (mandante ou visitante)
resultado_clube as (
    select clube_casa_id as clube_id, rodada_id, resultado_mandante as resultado, placar_mandante as gols_marcados, placar_visitante as gols_sofridos from partidas where placar_mandante is not null
    union all
    select clube_visitante_id, rodada_id, resultado_visitante, placar_visitante, placar_mandante from partidas where placar_visitante is not null
),

final as (
    select
        c.clube_id,
        c.rodada_id,
        c.total_pontos_cartola,
        c.media_pontos_cartola,
        coalesce(r.gols_marcados, 0)    as total_gols_marcados,
        coalesce(r.gols_sofridos, 0)    as total_gols_sofridos,
        coalesce(r.gols_marcados, 0) - coalesce(r.gols_sofridos, 0) as saldo_gols,
        r.resultado,
        t.posicao                       as posicao_tabela,
        t.pontos                        as pontos_campeonato,
        t.aproveitamento_percentual,
        current_timestamp()             as _updated_at
    from cartola_metrics c
    left join resultado_clube r on c.clube_id = r.clube_id and c.rodada_id = r.rodada_id
    left join tabela t          on c.clube_id = t.clube_id and c.rodada_id = t.rodada_id
)

select * from final
