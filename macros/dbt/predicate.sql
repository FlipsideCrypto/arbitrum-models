{% macro between_predicate(input_column='block_number') %}
    {% set database_name = target.database %}
    {% set schema_name = generate_schema_name(node=model) %}
    {% set table_name = generate_alias_name(node=model) %}
    {{- database_name }}.{{ schema_name }}.{{ table_name }}.{{ input_column }} BETWEEN 
    (SELECT MIN({{ input_column }}) FROM {{ database_name }}.{{ schema_name }}.{{ table_name }}__dbt_tmp)
    AND 
    (SELECT MAX({{ input_column }}) FROM {{ database_name }}.{{ schema_name }}.{{ table_name }}__dbt_tmp)
{%- endmacro %}