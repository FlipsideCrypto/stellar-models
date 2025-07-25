{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = 'complete_token_asset_metadata_id',
    tags = ['silver_prices','scheduled_core']
) }}

WITH providers AS (

    SELECT
        asset_id AS provider_asset_id,
        UPPER(
            CASE
                WHEN len(TRIM(token_address)) = 56 THEN token_address
                ELSE SPLIT_PART(
                    token_address,
                    '-',
                    2
                )
            END
        ) AS asset_issuer,
        UPPER(
            CASE
                WHEN token_address LIKE '%-%' THEN SPLIT_PART(
                    token_address,
                    '-',
                    1
                )
                ELSE symbol
            END
        ) AS asset_code,
        NAME,
        UPPER(symbol) AS symbol,
        decimals,
        blockchain,
        blockchain_name,
        blockchain_id,
        is_deprecated,
        provider,
        source,
        is_verified,
        _inserted_timestamp
    FROM
        {{ ref(
            'bronze__complete_token_asset_metadata'
        ) }} A
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

qualify (ROW_NUMBER() over (PARTITION BY asset_issuer, asset_code
ORDER BY
    provider, _inserted_timestamp DESC)) = 1
)
SELECT
    A.provider_asset_id,
    A.asset_issuer,
    A.asset_code,
    COALESCE(
        b.asset_id,
        b2.asset_id
    ) AS asset_id,
    A.name,
    A.symbol,
    A.decimals,
    A.blockchain,
    A.blockchain_name,
    A.blockchain_id,
    A.is_deprecated,
    A.provider,
    A.source,
    A.is_verified,
    A._inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['a.asset_issuer','a.asset_code']) }} AS complete_token_asset_metadata_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    providers A
    LEFT JOIN {{ ref('silver__assets') }}
    b
    ON A.asset_issuer = b.asset_issuer
    AND A.asset_code = b.asset_code
    LEFT JOIN {{ ref('silver__assets') }}
    b2
    ON A.asset_issuer = b2.asset_issuer
    AND A.symbol = b2.asset_code
