{% macro coverage_score(previsao_col, real_col, threshold) %}
    avg(
        case
            when abs({{ previsao_col }} - {{ real_col }}) <= {{ threshold }} then 1.0
            else 0.0
        end
    )
{% endmacro %}
