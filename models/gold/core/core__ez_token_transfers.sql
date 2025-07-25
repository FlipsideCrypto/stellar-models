-- depends_on: {{ ref('silver__transactions') }}
{{ config(
    materialized = 'incremental',
    unique_key = ["ez_token_transfers_id"],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE','closed_at::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(transaction_hash,operation_id,from_address,to_address,contract_id,event_topic);",
    tags = ['scheduled_core']
) }}

SELECT
    transaction_hash,
    ledger_sequence,
    operation_id,
    closed_at AS block_timestamp,
    from_address,
    to_address,
    amount,
    amount_raw,
    amount * COALESCE(
        b2.price,
        b.price
    ) AS amount_usd,
    A.asset,
    A.asset_code,
    A.asset_issuer,
    A.asset_type,
    COALESCE(
        b.is_verified,
        b2.is_verified,
        FALSE
    ) AS token_is_verified,
    contract_id,
    event_topic,
    to_muxed,
    to_muxed_id,
    transaction_id,
    closed_at,
    batch_id,
    batch_run_date,
    batch_insert_ts,
    token_transfers_id AS ez_token_transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ ref('silver__token_transfers') }} A
    LEFT JOIN {{ ref('price__ez_prices_hourly') }}
    b
    ON DATE_TRUNC(
        'hour',
        A.closed_at
    ) = b.hour
    AND A.asset_issuer = b.asset_issuer
    AND A.asset_code = b.asset_code
    LEFT JOIN {{ ref('price__ez_prices_hourly') }}
    b2
    ON DATE_TRUNC(
        'hour',
        A.closed_at
    ) = b2.hour
    AND A.asset_type = 'native'
    AND b2.is_native
    LEFT JOIN {{ ref('core__dim_assets') }} C
    ON A.asset_issuer = C.asset_issuer
    AND A.asset_code = C.asset_code

{% if is_incremental() %}
WHERE
    A.modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
