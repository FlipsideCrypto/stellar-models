{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'fact_prices_ohlc_hourly_id',
    cluster_by = ['hour::DATE','provider'],
    tags = ['gold_prices','scheduled_core']
) }}

SELECT
    provider_asset_id,
    recorded_hour AS HOUR,
    OPEN,
    high,
    low,
    CLOSE,
    provider,
    {{ dbt_utils.generate_surrogate_key(['complete_provider_prices_id']) }} AS fact_prices_ohlc_hourly_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ ref('silver__complete_provider_prices') }}

{% if is_incremental() %}
WHERE
    modified_timestamp > (
        SELECT
            COALESCE(MAX(modified_timestamp), '1970-01-01' :: TIMESTAMP) AS modified_timestamp
        FROM
            {{ this }})
        {% endif %}
