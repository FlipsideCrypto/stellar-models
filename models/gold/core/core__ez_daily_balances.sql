{{ config(
    materialized = 'incremental',
    unique_key = ['ez_daily_balances_id'],
    incremental_strategy = 'merge',
    merge_exclude_columns = ['inserted_timestamp'],
    cluster_by = ['balance_date::DATE','asset_issuer||asset_code'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(account_id); DELETE FROM {{ this }} WHERE balance_date < SYSDATE() :: DATE - 101;",
    tags = ['scheduled_daily']
) }}

{% if execute %}

{% if is_incremental() %}
{% set min_balance_date_query %}

SELECT
    COALESCE(MIN(balance_date), CURRENT_DATE()) AS min_balance_date
FROM
    {{ ref('core__fact_daily_balances') }}
WHERE
    modified_timestamp > (
        SELECT
            COALESCE(MAX(modified_timestamp), '1970-01-01' :: TIMESTAMP) AS max_modified_timestamp
        FROM
            {{ this }}) {% endset %}
            {% set min_balance_date_result = run_query(min_balance_date_query) %}
            {% set min_balance_date = min_balance_date_result [0] [0] %}
        {% endif %}
        {% endif %}

        WITH daily_balances AS (
            SELECT
                balance_date,
                account_id,
                asset_issuer,
                asset_code,
                balance,
                is_deleted,
                balance_changed_on_date,
                daily_balance_change,
                daily_balance_id,
                inserted_timestamp,
                modified_timestamp
            FROM
                {{ ref('core__fact_daily_balances') }}
            WHERE
                balance > 0
                AND balance_date >= SYSDATE() :: DATE -101

{% if is_incremental() %}
AND balance_date >= '{{ min_balance_date }}' :: DATE
{% endif %}
),
hourly_prices AS (
    SELECT
        HOUR :: DATE AS price_date,
        UPPER(asset_issuer) AS asset_issuer,
        UPPER(asset_code) AS asset_code,
        symbol,
        decimals,
        is_native,
        is_verified,
        price AS last_price_of_day
    FROM
        {{ ref('price__ez_prices_hourly') }}

{% if is_incremental() %}
WHERE
    HOUR :: DATE >= '{{ min_balance_date }}' :: DATE
{% endif %}

qualify ROW_NUMBER() over (
    PARTITION BY price_date,
    UPPER(asset_issuer),
    UPPER(asset_code)
    ORDER BY
        HOUR DESC
) = 1
)
SELECT
    db.balance_date,
    db.account_id,
    NULLIF(
        db.asset_issuer,
        'native'
    ) AS asset_issuer,
    NULLIF(
        db.asset_code,
        'native'
    ) AS asset_code,
    p.symbol,
    db.balance,
    db.balance * p.last_price_of_day AS balance_usd,
    db.daily_balance_change,
    db.daily_balance_change * p.last_price_of_day AS daily_balance_change_usd,
    db.balance_changed_on_date,
    p.is_native,
    p.is_verified AS token_is_verified,
    db.daily_balance_id AS ez_daily_balances_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    daily_balances db
    LEFT JOIN hourly_prices p
    ON db.balance_date = p.price_date
    AND (
        (
            db.asset_issuer = p.asset_issuer
            AND db.asset_code = p.asset_code
        )
        OR (
            db.asset_issuer = 'native'
            AND db.asset_code = 'native'
            AND p.is_native = TRUE
        )
    )
