{% macro calcular_resultado(gols_clube, gols_adversario) %}
    case
        when {{ gols_clube }} > {{ gols_adversario }} then 'V'
        when {{ gols_clube }} = {{ gols_adversario }} then 'E'
        when {{ gols_clube }} < {{ gols_adversario }} then 'D'
        else null
    end
{% endmacro %}
