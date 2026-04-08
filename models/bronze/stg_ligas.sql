with source as (
    select * from {{ source('bronze', 'raw_ligas') }}
)

select
    liga_id,
    slug,
    nome,
    descricao,
    tipo,
    imagem,
    criacao,
    quantidade_times,
    vagas_restantes,
    mata_mata,
    sem_capitao,
    _ingestao_timestamp,
    _batch_id
from source
