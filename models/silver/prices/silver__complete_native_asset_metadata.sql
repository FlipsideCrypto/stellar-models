{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = 'complete_native_asset_metadata_id',
    tags = ['silver_prices','scheduled_core']
) }}

SELECT
    asset_id,
    symbol,
    NAME,
    decimals,
    blockchain,
    is_deprecated,
    provider,
    source,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['complete_native_asset_metadata_id']) }} AS complete_native_asset_metadata_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref(
        'bronze__complete_native_asset_metadata'
    ) }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(
                modified_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
