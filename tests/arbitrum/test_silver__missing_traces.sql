-- depends_on: {{ ref('test_silver__transactions_full') }}
{{ missing_traces(ref("test_silver__transactions_full"), ref("test_silver__traces_full")) }}
