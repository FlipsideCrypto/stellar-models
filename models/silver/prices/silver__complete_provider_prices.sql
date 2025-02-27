{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = 'complete_provider_prices_id',
    cluster_by = ['recorded_hour::DATE','provider'],
    tags = ['silver_prices','scheduled_core']
) }}

SELECT
    p.asset_id AS provider_asset_id,
    recorded_hour,
    OPEN,
    high,
    low,
    CLOSE,
    p.provider,
    p.source,
    p._inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['p.complete_provider_prices_id']) }} AS complete_provider_prices_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref(
        'bronze__complete_provider_prices'
    ) }}
    p
    INNER JOIN {{ ref('bronze__complete_provider_asset_metadata') }}
    m
    ON p.asset_id = m.asset_id

{% if is_incremental() %}
WHERE
    p.modified_timestamp >= (
        SELECT
            MAX(
                modified_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY p.asset_id, recorded_hour, p.provider
ORDER BY
    p.modified_timestamp DESC)) = 1
