-- Garante que placares registrados não são negativos.
-- Retorna partidas com placar inválido (teste falha se retornar algo).

select
    partida_id,
    rodada_id,
    placar_mandante,
    placar_visitante
from {{ ref('fct_partidas') }}
where
    (placar_mandante  is not null and placar_mandante  < 0)
 or (placar_visitante is not null and placar_visitante < 0)
