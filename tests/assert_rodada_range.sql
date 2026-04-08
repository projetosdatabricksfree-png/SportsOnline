-- Garante que todas as rodadas estão no range válido do Brasileirão (1 a 38).
-- Retorna rodadas fora do range (teste falha se retornar algo).

select distinct rodada_id
from {{ ref('fct_atletas_rodada') }}
where rodada_id < 1 or rodada_id > 38
