{{
    config(
        materialized='incremental',
        unique_key=['atleta_id', 'rodada_alvo'],
        incremental_strategy='merge'
    )
}}

-- Previsão final do meta-modelo com ranking por posição.
-- Populada pelo notebook 05_meta_modelo.py.

with previsao as (
    select * from {{ source('models_ml', 'raw_previsao_final') }}
    {% if is_incremental() %}
    where rodada_alvo >= (select max(rodada_alvo) from {{ this }})
    {% endif %}
),

atletas as (
    select atleta_id, apelido, posicao_id, clube_id, preco_atual
    from {{ ref('metricas_atleta_acumulado') }}
),

posicoes as (
    select posicao_id, abreviacao from {{ ref('dim_posicoes') }}
),

clubes as (
    select clube_id, abreviacao as clube_abreviacao from {{ ref('dim_clubes') }}
),

final as (
    select
        p.atleta_id,
        a.apelido,
        po.abreviacao                           as posicao,
        c.clube_abreviacao                      as clube,
        p.rodada_alvo,
        p.pontuacao_prevista,
        p.modelo_selecionado,
        p.coverage_score_modelo,
        a.preco_atual,
        case
            when a.preco_atual > 0
            then round(p.pontuacao_prevista / a.preco_atual, 2)
            else null
        end                                     as pontos_por_cartoleta,
        rank() over (
            partition by p.rodada_alvo, a.posicao_id
            order by p.pontuacao_prevista desc
        )                                       as ranking_posicao,
        p._generated_at
    from previsao p
    join atletas  a  on p.atleta_id = a.atleta_id
    join posicoes po on a.posicao_id = po.posicao_id
    join clubes   c  on a.clube_id   = c.clube_id
)

select * from final
