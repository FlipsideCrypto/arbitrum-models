{% macro add_database_or_schema_tags() %}
    {{ set_database_tag_value('BLOCKCHAIN_NAME','ARBITRUM') }}
    {{ set_database_tag_value('BLOCKCHAIN_TYPE','EVM, L2') }}
{% endmacro %}