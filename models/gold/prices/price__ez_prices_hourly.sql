{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = 'ez_prices_hourly_id',
    cluster_by = ['hour::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(asset_issuer, asset_code, asset_id, symbol, name)",
    tags = ['gold_prices','scheduled_core']
) }}

SELECT
    HOUR,
    asset_issuer,
    asset_code,
    asset_id,
    price,
    symbol,
    decimals,
    blockchain,
    FALSE AS is_native,
    is_imputed,
    is_deprecated,
    {{ dbt_utils.generate_surrogate_key(['complete_token_prices_id']) }} AS ez_prices_hourly_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ ref('silver__complete_token_prices') }}

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
            HOUR,
            NULL AS asset_issuer,
            NULL AS asset_code,
            -5706705804583548011 AS asset_id,
            NAME,
            symbol,
            decimals,
            blockchain,
            TRUE AS is_native,
            is_imputed,
            is_deprecated,
            {{ dbt_utils.generate_surrogate_key(['complete_native_prices_id']) }} AS ez_prices_hourly_id,
            SYSDATE() AS inserted_timestamp,
            SYSDATE() AS modified_timestamp
        FROM
            {{ ref('silver__complete_native_prices') }}

{% if is_incremental() %}
WHERE
    modified_timestamp > (
        SELECT
            COALESCE(MAX(modified_timestamp), '1970-01-01' :: TIMESTAMP) AS modified_timestamp
        FROM
            {{ this }})
        {% endif %}
