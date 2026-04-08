-- Garante que cada atleta aparece uma única vez por rodada na tabela fct_atletas_rodada.
-- Retorna linhas com duplicatas (teste falha se retornar algo).

select
    atleta_id,
    rodada_id,
    count(*) as qtd
from {{ ref('fct_atletas_rodada') }}
group by atleta_id, rodada_id
having count(*) > 1
