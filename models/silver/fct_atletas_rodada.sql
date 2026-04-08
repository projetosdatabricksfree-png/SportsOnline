{{
    config(
        materialized='incremental',
        unique_key=['atleta_id', 'rodada_id'],
        incremental_strategy='merge',
        partition_by={
            'field': 'rodada_id',
            'data_type': 'int'
        }
    )
}}

with mercado as (
    select * from {{ ref('stg_atletas_mercado') }}
    {% if is_incremental() %}
    where _rodada_captura >= (select max(rodada_id) - 1 from {{ this }})
    {% endif %}
),

pontuados as (
    select * from {{ ref('stg_atletas_pontuados') }}
    {% if is_incremental() %}
    where rodada_id >= (select max(rodada_id) - 1 from {{ this }})
    {% endif %}
),

-- Combina dados do mercado com pontuação real quando disponível
combinado as (
    select
        m.atleta_id,
        m._rodada_captura                              as rodada_id,
        m.clube_id,
        m.posicao_id,
        m.status_id,
        coalesce(p.pontuacao, cast(m.pontos_num as float)) as pontos_num,
        m.media_num,
        m.preco_num,
        m.variacao_num,
        m.jogos_num,
        m.entrou_em_campo,
        coalesce(p.scout_json, m.scout_json)           as scout_json,
        m._ingestao_timestamp
    from mercado m
    left join pontuados p
        on m.atleta_id = p.atleta_id
        and m._rodada_captura = p.rodada_id
),

final as (
    select
        atleta_id,
        rodada_id,
        clube_id,
        posicao_id,
        status_id,
        pontos_num,
        media_num,
        preco_num,
        variacao_num,
        jogos_num,
        entrou_em_campo,
        {{ parse_scout_json('scout_json') }},
        _ingestao_timestamp
    from combinado
)

select * from final
