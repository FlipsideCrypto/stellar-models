{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = 'ez_asset_metadata_id',
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(asset_issuer, asset_code, asset_id, symbol, name)",
    tags = ['gold_prices','scheduled_core']
) }}

SELECT
    provider_asset_id,
    asset_issuer,
    asset_code,
    asset_id,
    NAME,
    symbol,
    decimals,
    blockchain,
    FALSE AS is_native,
    COALESCE(
        is_verified,
        FALSE
    ) AS is_verified,
    is_deprecated,
    {{ dbt_utils.generate_surrogate_key(['complete_token_asset_metadata_id']) }} AS ez_asset_metadata_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ ref('silver__complete_token_asset_metadata') }}

{% if is_incremental() %}
WHERE
    modified_timestamp > (
        SELECT
            COALESCE(MAX(modified_timestamp), '1970-01-01' :: TIMESTAMP) AS modified_timestamp
        FROM
            {{ this }})
        {% endif %}
        UNION ALL
        SELECT
            asset_id AS provider_asset_id,
            NULL AS asset_issuer,
            NULL AS asset_code,
            -5706705804583548011 AS asset_id,
            NAME,
            symbol,
            decimals,
            blockchain,
            TRUE AS is_native,
            is_deprecated,
            TRUE AS is_verified,
            {{ dbt_utils.generate_surrogate_key(['complete_native_asset_metadata_id']) }} AS ez_asset_metadata_id,
            SYSDATE() AS inserted_timestamp,
            SYSDATE() AS modified_timestamp
        FROM
            {{ ref('silver__complete_native_asset_metadata') }}

{% if is_incremental() %}
WHERE
    modified_timestamp > (
        SELECT
            COALESCE(MAX(modified_timestamp), '1970-01-01' :: TIMESTAMP) AS modified_timestamp
        FROM
            {{ this }})
        {% endif %}
