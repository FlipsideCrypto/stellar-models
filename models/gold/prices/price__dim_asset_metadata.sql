{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = 'dim_asset_metadata_id',
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(asset_issuer, asset_code,symbol, name) ",
    tags = ['gold_prices','scheduled_core']
) }}

SELECT
    provider_asset_id,
    asset_issuer,
    asset_code,
    NAME,
    symbol,
    platform AS blockchain,
    platform_id AS blockchain_id,
    provider,
    {{ dbt_utils.generate_surrogate_key(['complete_provider_asset_metadata_id']) }} AS dim_asset_metadata_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ ref('silver__complete_provider_asset_metadata') }}

{% if is_incremental() %}
WHERE
    modified_timestamp > (
        SELECT
            COALESCE(MAX(modified_timestamp), '1970-01-01' :: TIMESTAMP) AS modified_timestamp
        FROM
            {{ this }})
        {% endif %}
