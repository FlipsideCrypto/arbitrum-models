{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = ['block_number'],
    cluster_by = "block_timestamp::date",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
    tags = ['full_reload_traces'],
    full_refresh = false
) }}
{{ gold_traces(
    full_reload_start_block = 25000000,
    full_reload_blocks = 10000000,
    full_reload_mode = true,
    uses_overflow_steps = false,
    arb_traces_mode = true
) }}
