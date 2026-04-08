{{
    config(
        materialized='incremental',
        unique_key=['atleta_id', 'rodada_alvo'],
        incremental_strategy='merge',
        partition_by={
            'field': 'rodada_alvo',
            'data_type': 'int'
        }
    )
}}

-- Feature Store para os modelos ML.
-- Gerado antes de cada rodada (pontuacao_real = NULL).
-- Atualizado após a rodada com pontuacao_real preenchida via stg_atletas_pontuados.

with atletas as (
    select * from {{ ref('dim_atletas') }}
),

metricas as (
    select * from {{ ref('metricas_atleta_acumulado') }}
),

metricas_clube as (
    select * from {{ ref('metricas_clube_rodada') }}
),

tabela as (
    select * from {{ ref('tabela_brasileirao') }}
),

partidas_proxima as (
    select * from {{ ref('fct_partidas') }}
    where rodada_id = (select max(rodada_id) from {{ ref('fct_partidas') }})
),

pontuados as (
    select * from {{ ref('stg_atletas_pontuados') }}
),

classicos as (
    select clube_id_a, clube_id_b from {{ ref('seed_classicos') }}
),

rodada_alvo_val as (
    select max(rodada_id) + 1 as rodada_alvo
    from {{ ref('fct_atletas_rodada') }}
),

-- Determina o adversário de cada atleta na próxima rodada
adversario_por_atleta as (
    select
        a.atleta_id,
        a.clube_id,
        case
            when p.clube_casa_id = a.clube_id     then 'casa'
            when p.clube_visitante_id = a.clube_id then 'fora'
        end                                         as mando_campo,
        case
            when p.clube_casa_id = a.clube_id     then p.clube_visitante_id
            when p.clube_visitante_id = a.clube_id then p.clube_casa_id
        end                                         as adversario_id
    from atletas a
    left join partidas_proxima p
        on a.clube_id in (p.clube_casa_id, p.clube_visitante_id)
),

tabela_max as (
    select *
    from tabela
    qualify row_number() over (partition by clube_id order by rodada_id desc) = 1
),

final as (
    select
        m.atleta_id,
        rv.rodada_alvo,
        -- Features do atleta
        m.media_pontos                              as media_pontos_geral,
        m.media_ultimas_5                           as media_ultimas_5,
        coalesce(
            avg(f.pontos_num) over (
                partition by m.atleta_id
                order by f.rodada_id desc
                rows between 2 preceding and current row
            ), m.media_pontos
        )                                           as media_ultimas_3,
        m.desvio_padrao_pontos,
        m.consistencia_score,
        m.total_jogos,
        m.aproveitamento_campo,
        m.preco_atual,
        m.variacao_preco_acumulada                  as variacao_preco,
        m.posicao_id,
        -- Features do clube do atleta
        tm_clube.posicao                            as clube_posicao_tabela,
        tm_clube.aproveitamento_percentual          as clube_aproveitamento,
        mc.media_pontos_cartola                     as clube_media_cartola,
        -- Features do adversário
        adv.adversario_id,
        tm_adv.posicao                              as adversario_posicao_tabela,
        mc_adv.media_pontos_cartola                 as adversario_media_cartola,
        -- Contexto
        adv.mando_campo,
        case
            when exists (
                select 1 from classicos c
                where (c.clube_id_a = m.clube_id and c.clube_id_b = adv.adversario_id)
                   or (c.clube_id_b = m.clube_id and c.clube_id_a = adv.adversario_id)
            ) then true else false
        end                                         as eh_classico,
        rv.rodada_alvo                              as rodada_id_relativa,
        -- Target (NULL pré-rodada, preenchido pós-rodada)
        p.pontuacao                                 as pontuacao_real,
        current_timestamp()                         as _generated_at
    from metricas m
    cross join rodada_alvo_val rv
    left join {{ ref('fct_atletas_rodada') }} f on m.atleta_id = f.atleta_id
    left join adversario_por_atleta adv         on m.atleta_id = adv.atleta_id
    left join tabela_max tm_clube               on m.clube_id  = tm_clube.clube_id
    left join tabela_max tm_adv                 on adv.adversario_id = tm_adv.clube_id
    left join metricas_clube mc                 on m.clube_id  = mc.clube_id
        and mc.rodada_id = (select max(rodada_id) from metricas_clube)
    left join metricas_clube mc_adv             on adv.adversario_id = mc_adv.clube_id
        and mc_adv.rodada_id = (select max(rodada_id) from metricas_clube)
    left join pontuados p                       on m.atleta_id = p.atleta_id
        and p.rodada_id = rv.rodada_alvo
    {% if is_incremental() %}
    where rv.rodada_alvo >= (select max(rodada_alvo) from {{ this }})
    {% endif %}
)

select * from final
