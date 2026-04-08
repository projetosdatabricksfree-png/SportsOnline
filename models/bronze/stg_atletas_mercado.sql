with source as (
    select * from {{ source('bronze', 'raw_atletas_mercado') }}
)

select
    atleta_id,
    nome,
    apelido,
    apelido_abreviado,
    slug,
    foto                as foto_url,
    clube_id,
    posicao_id,
    status_id,
    rodada_id,
    pontos_num,
    media_num,
    preco_num,
    variacao_num,
    jogos_num,
    entrou_em_campo,
    scout_json,
    _rodada_captura,
    _ingestao_timestamp,
    _batch_id
from source
