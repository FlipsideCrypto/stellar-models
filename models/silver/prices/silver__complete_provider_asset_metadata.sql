{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = 'complete_provider_asset_metadata_id',
    tags = ['silver_prices','scheduled_core']
) }}

WITH base AS(

    SELECT
        asset_id AS provider_asset_id,
        SPLIT_PART(
            token_address,
            '-',
            1
        ) seg1,
        SPLIT_PART(
            token_address,
            '-',
            2
        ) seg2,
        UPPER(
            CASE
                WHEN len(TRIM(token_address)) = 56 THEN token_address
                WHEN token_address ILIKE '%-%'
                AND len(seg1) > len(seg2) THEN seg1
                WHEN token_address ILIKE '%-%'
                AND len(seg2) > len(seg1) THEN seg2
            END
        ) AS asset_issuer,
        UPPER(
            CASE
                WHEN token_address ILIKE '%-%'
                AND len(seg1) > len(seg2) THEN seg2
                WHEN token_address ILIKE '%-%'
                AND len(seg2) > len(seg1) THEN seg1
                ELSE symbol
            END
        ) AS asset_code,
        NAME,
        UPPER(symbol) AS symbol,
        platform,
        platform_id,
        provider,
        source,
        _inserted_timestamp,
        complete_provider_asset_metadata_id
    FROM
        {{ ref(
            'bronze__complete_provider_asset_metadata'
        ) }}
    WHERE
        len(asset_issuer) > 0

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(
            modified_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    provider_asset_id,
    asset_issuer,
    asset_code,
    NAME,
    symbol,
    platform,
    platform_id,
    provider,
    source,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['complete_provider_asset_metadata_id']) }} AS complete_provider_asset_metadata_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base
