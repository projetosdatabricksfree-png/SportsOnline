{{
    config(
        materialized='table'
    )
}}

with fatos as (
    select * from {{ ref('fct_atletas_rodada') }}
    where entrou_em_campo = true
),

atletas as (
    select * from {{ ref('dim_atletas') }}
),

-- Últimas 5 rodadas por atleta para calcular tendência
ultimas_5 as (
    select
        atleta_id,
        rodada_id,
        pontos_num,
        preco_num,
        row_number() over (partition by atleta_id order by rodada_id desc) as rn_desc
    from fatos
),

media_ultimas_5 as (
    select
        atleta_id,
        avg(pontos_num) as media_ultimas_5,
        max(preco_num)  as preco_atual
    from ultimas_5
    where rn_desc <= 5
    group by atleta_id
),

media_ultimas_3 as (
    select
        atleta_id,
        avg(pontos_num) as media_ultimas_3
    from ultimas_5
    where rn_desc <= 3
    group by atleta_id
),

acumulado as (
    select
        f.atleta_id,
        f.clube_id,
        f.posicao_id,
        count(*)                                        as total_jogos,
        sum(f.pontos_num)                               as total_pontos,
        avg(f.pontos_num)                               as media_pontos,
        stddev(f.pontos_num)                            as desvio_padrao_pontos,
        max(f.pontos_num)                               as max_pontos,
        min(f.pontos_num)                               as min_pontos,
        percentile(f.pontos_num, 0.5)                   as mediana_pontos,
        sum(f.scout_gol)                                as total_gols,
        sum(f.scout_assistencia)                        as total_assistencias,
        sum(f.scout_desarme)                            as total_desarmes,
        sum(f.scout_cartao_amarelo)                     as total_cartoes_amarelos,
        sum(f.scout_cartao_vermelho)                    as total_cartoes_vermelhos,
        sum(f.scout_saldo_gol)                          as total_saldo_gol,
        sum(f.scout_defesa)                             as total_defesas,
        sum(f.scout_finalizacao_trave)                  as total_finalizacoes_trave,
        avg(cast(f.entrou_em_campo as int))             as aproveitamento_campo,
        sum(f.variacao_num)                             as variacao_preco_acumulada
    from fatos f
    group by f.atleta_id, f.clube_id, f.posicao_id
),

final as (
    select
        a.atleta_id,
        atl.apelido,
        a.clube_id,
        a.posicao_id,
        {{ var('temporada') }}                          as temporada,
        -- Métricas acumuladas
        a.total_jogos,
        a.total_pontos,
        a.media_pontos,
        a.desvio_padrao_pontos,
        a.max_pontos,
        a.min_pontos,
        a.mediana_pontos,
        a.total_gols,
        a.total_assistencias,
        a.total_desarmes,
        a.total_cartoes_amarelos,
        a.total_cartoes_vermelhos,
        a.total_saldo_gol,
        a.total_defesas,
        a.total_finalizacoes_trave,
        -- Indicadores derivados
        case when a.total_jogos > 0 then a.total_gols / a.total_jogos        else 0 end as gols_por_jogo,
        case when a.total_jogos > 0 then a.total_assistencias / a.total_jogos else 0 end as assistencias_por_jogo,
        case when a.total_jogos > 0
             then (a.total_gols + a.total_assistencias) / a.total_jogos
             else 0 end                                                        as participacao_gols_por_jogo,
        case when a.total_jogos > 0
             then (a.total_cartoes_amarelos + a.total_cartoes_vermelhos) / a.total_jogos
             else 0 end                                                        as cartoes_por_jogo,
        a.aproveitamento_campo,
        -- Preço
        coalesce(m5.preco_atual, 0)                     as preco_atual,
        a.variacao_preco_acumulada,
        case when a.media_pontos > 0 then coalesce(m5.preco_atual, 0) / a.media_pontos else null end
                                                         as preco_por_ponto,
        -- Tendência
        coalesce(m5.media_ultimas_5, a.media_pontos)    as media_ultimas_5,
        {{ calcular_tendencia(
            'coalesce(m5.media_ultimas_5, a.media_pontos)',
            'coalesce(m3.media_ultimas_3, a.media_pontos)'
        ) }}                                             as tendencia_pontos,
        -- Consistência: quanto mais próximo de 1, mais consistente
        case
            when a.media_pontos > 0
            then 1 - (coalesce(a.desvio_padrao_pontos, 0) / a.media_pontos)
            else 0
        end                                              as consistencia_score,
        current_timestamp()                              as _updated_at
    from acumulado a
    join atletas atl on a.atleta_id = atl.atleta_id
    left join media_ultimas_5 m5 on a.atleta_id = m5.atleta_id
    left join media_ultimas_3 m3 on a.atleta_id = m3.atleta_id
)

select * from final
