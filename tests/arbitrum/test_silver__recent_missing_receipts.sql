-- depends_on: {{ ref('test_silver__transactions_recent') }}
{{ recent_missing_traces(ref("test_silver__receipts_recent")) }}
