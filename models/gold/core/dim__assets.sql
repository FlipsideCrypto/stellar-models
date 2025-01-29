{{ config(
    materialized = 'incremental',
    unique_key = ['dim_assets_id'],
    incremental_strategy = 'merge',
    merge_exclude_columns = ['inserted_timestamp'],
    tags = ['core']
) }}

SELECT
    asset_type,
    asset_code,
    asset_issuer,
    asset_id as asset_id_hash,
    {{ dbt_utils.generate_surrogate_key(
        ['asset_id']
    ) }} AS dim_assets_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('silver__assets') }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
