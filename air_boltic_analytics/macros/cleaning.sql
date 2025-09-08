{% macro nullif_blank(column) %}
    nullif(trim({{ column }}), '')
{% endmacro %}


{% macro standardize_phone(column) %}
    regexp_replace({{ column }}, '[^0-9]+', '')
{% endmacro %}


{% macro to_bool(column) %}
    case
        when lower(trim({{ column }})) in ('true','t','1','yes','y') then true
        when lower(trim({{ column }})) in ('false','f','0','no','n') then false
    else null
    end
{% endmacro %}


{% macro parse_ts(column, fmt=None) %}
    {%- if fmt is none -%}
        to_timestamp({{ column }})
    {%- else -%}
        to_timestamp({{ column }}, '{{ fmt }}')
    {%- endif -%}
{% endmacro %}


{% macro surrogate_key(cols) -%}
    md5(concat_ws('||',
        {%- for col in cols -%}
            cast(coalesce({{ col }}, '') as string){% if not loop.last %}, {% endif %}
        {%- endfor -%}
    ))
{%- endmacro %}
