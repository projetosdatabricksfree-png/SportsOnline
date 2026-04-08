{% snapshot snap_preco_atletas %}

{{
    config(
        target_schema='silver',
        target_database='cartola_fc',
        unique_key='atleta_id',
        strategy='check',
        check_cols=['preco_num', 'clube_id', 'status_id']
    )
}}

select
    atleta_id,
    apelido,
    clube_id,
    posicao_id,
    status_id,
    preco_num,
    media_num,
    variacao_num,
    _rodada_captura as rodada_id,
    _ingestao_timestamp
from {{ ref('stg_atletas_mercado') }}

{% endsnapshot %}
