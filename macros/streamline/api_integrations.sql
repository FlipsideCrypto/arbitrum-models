{% macro create_aws_arbitrum_api() %}
    {% if target.name == "prod" %}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_arbitrum_api api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::490041342817:role/snowflake-api-arbitrum' api_allowed_prefixes = (
            'https://XXX.execute-api.us-east-1.amazonaws.com/prod/',
            'https://lz7pjsdoa4.execute-api.us-east-1.amazonaws.com/dev/'
        ) enabled = TRUE;
{% endset %}
        {% do run_query(sql) %}
    {% endif %}
{% endmacro %}
