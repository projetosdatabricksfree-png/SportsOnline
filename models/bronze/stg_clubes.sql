with source as (
    select * from {{ source('bronze', 'raw_clubes') }}
)

select
    clube_id,
    nome,
    abreviacao,
    slug,
    apelido,
    nome_fantasia,
    url_editoria,
    escudo_60x60,
    escudo_45x45,
    escudo_30x30,
    _ingestao_timestamp,
    _batch_id
from source
