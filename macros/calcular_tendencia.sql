{% macro calcular_tendencia(media_atual, media_anterior) %}
    case
        when {{ media_atual }} > {{ media_anterior }} * 1.10 then 'subindo'
        when {{ media_atual }} < {{ media_anterior }} * 0.90 then 'caindo'
        else 'estavel'
    end
{% endmacro %}
