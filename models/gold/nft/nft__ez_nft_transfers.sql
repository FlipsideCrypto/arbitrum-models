{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'NFT' } } }
) }}

SELECT
    block_timestamp,
    block_number,
    tx_hash,
    event_index,
    tx_position, -- new 
    intra_event_index,
    event_type, -- deprecate 
    contract_address AS nft_address, -- deprecate 
    contract_address, -- new
    project_name, -- deprecate 
    project_name as name -- new
    from_address AS nft_from_address, -- deprecate
    to_address AS nft_to_address, -- deprecate
    from_address, -- new
    to_address, -- new 
    tokenId, -- deprecate
    tokenid as token_id, -- new
    token_standard, -- add  
    token_transfer_type, -- add 
    erc1155_value, -- deprecate
    erc1155_value as quantity, -- new
    origin_function_signature, --new
    origin_from_address, --new
    origin_to_address, --new
    COALESCE (
        nft_transfers_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash','event_index','intra_event_index']
        ) }}
    ) AS ez_nft_transfers_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__nft_transfers') }}
