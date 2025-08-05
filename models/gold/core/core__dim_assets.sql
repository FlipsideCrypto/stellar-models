-- depends_on: {{ ref('silver__assets') }}
{{ config(
    materialized = 'incremental',
    unique_key = ["asset_issuer","asset_code"],
    incremental_strategy = 'merge',
    merge_exclude_columns = ['inserted_timestamp'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(asset_code,asset_issuer,asset_id);",
    tags = ['scheduled_core']
) }}

SELECT
    asset_id,
    asset_type,
    asset_code,
    asset_issuer,
    id,
    {{ dbt_utils.generate_surrogate_key(
        ['asset_id']
    ) }} AS dim_assets_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
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
