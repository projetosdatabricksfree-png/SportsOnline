{{
    config(
        materialized='incremental',
        unique_key='partida_id',
        incremental_strategy='merge',
        partition_by={
            'field': 'rodada_id',
            'data_type': 'int'
        }
    )
}}

with source as (
    select * from {{ ref('stg_partidas') }}
    {% if is_incremental() %}
    where rodada_id >= (select max(rodada_id) - 1 from {{ this }})
    {% endif %}
),

final as (
    select
        partida_id,
        rodada_id,
        clube_casa_id,
        clube_visitante_id,
        clube_casa_posicao,
        clube_visitante_posicao,
        placar_oficial_mandante     as placar_mandante,
        placar_oficial_visitante    as placar_visitante,
        {{ calcular_resultado('placar_oficial_mandante', 'placar_oficial_visitante') }}
            as resultado_mandante,
        {{ calcular_resultado('placar_oficial_visitante', 'placar_oficial_mandante') }}
            as resultado_visitante,
        coalesce(placar_oficial_mandante, 0) + coalesce(placar_oficial_visitante, 0)
            as total_gols,
        cast(partida_data as timestamp) as partida_datetime,
        estadio,
        valida,
        aproveitamento_mandante     as aproveitamento_mandante_ultimos5,
        aproveitamento_visitante    as aproveitamento_visitante_ultimos5,
        _ingestao_timestamp         as _updated_at
    from source
)

select * from final
