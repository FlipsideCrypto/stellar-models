{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_timestamp_hour",
    cluster_by = ['block_timestamp_hour::DATE'],
    tags = ['scheduled_daily','stats']
) }}

{% if execute %}

{% if is_incremental() %}
{% set query %}

SELECT
    DATE_TRUNC('hour', MIN(block_timestamp_hour)) AS block_timestamp_hour
FROM
    (
        SELECT
            MIN(block_timestamp) block_timestamp_hour
        FROM
            {{ ref('core__fact_ledgers') }}
        WHERE
            modified_timestamp >= (
                SELECT
                    MAX(modified_timestamp)
                FROM
                    {{ this }}
            )
        UNION ALL
        SELECT
            MIN(block_timestamp) block_timestamp_hour
        FROM
            {{ ref('core__fact_transactions') }}
        WHERE
            modified_timestamp >= (
                SELECT
                    MAX(modified_timestamp)
                FROM
                    {{ this }}
            )
    ) {% endset %}
    {% set min_block_timestamp_hour = run_query(query).columns [0].values() [0] %}
{% endif %}

{% if not min_block_timestamp_hour or min_block_timestamp_hour == 'None' %}
    {% set min_block_timestamp_hour = '2099-01-01' %}
{% endif %}
{% endif %}

WITH blcks AS (
    SELECT
        DATE_TRUNC(
            'hour',
            block_timestamp
        ) AS block_timestamp_hour,
        MIN(SEQUENCE) AS block_number_min,
        MAX(SEQUENCE) AS block_number_max,
        COUNT(1) AS block_count,
        SUM(transaction_count) AS transaction_count
    FROM
        {{ ref('core__fact_ledgers') }}
    WHERE
        block_timestamp_hour < DATE_TRUNC('hour', CURRENT_TIMESTAMP)

{% if is_incremental() %}
AND DATE_TRUNC(
    'hour',
    block_timestamp
) >= '{{ min_block_timestamp_hour }}'
{% endif %}
GROUP BY
    1
),
txs AS (
    SELECT
        DATE_TRUNC(
            'hour',
            block_timestamp
        ) AS block_timestamp_hour,
        COUNT(
            DISTINCT CASE
                WHEN SUCCESSFUL THEN id
            END
        ) AS transaction_count_success,
        COUNT(
            DISTINCT CASE
                WHEN NOT SUCCESSFUL THEN id
            END
        ) AS transaction_count_failed,
        COUNT(
            DISTINCT account
        ) AS unique_accounts_count,
        SUM(fee_charged) AS total_fees,
        {{ dbt_utils.generate_surrogate_key(['block_timestamp_hour']) }} AS core_metrics_hourly_id,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp,
        '{{ invocation_id }}' AS _invocation_id
    FROM
        {{ ref('core__fact_transactions') }}
    WHERE
        block_timestamp_hour < DATE_TRUNC('hour', CURRENT_TIMESTAMP)

{% if is_incremental() %}
AND DATE_TRUNC(
    'hour',
    block_timestamp
) >= '{{ min_block_timestamp_hour }}'
{% endif %}
GROUP BY
    1
)
SELECT
    A.block_timestamp_hour,
    A.block_number_min,
    A.block_number_max,
    A.block_count,
    A.transaction_count,
    COALESCE(
        b.transaction_count_success,
        0
    ) AS transaction_count_success,
    COALESCE(
        b.transaction_count_failed,
        0
    ) AS transaction_count_failed,
    COALESCE(
        b.unique_accounts_count,
        0
    ) AS unique_accounts_count,
    COALESCE(
        b.total_fees,
        0
    ) AS total_fees,
    {{ dbt_utils.generate_surrogate_key(['a.block_timestamp_hour']) }} AS core_metrics_hourly_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    blcks A
    LEFT JOIN txs b
    ON A.block_timestamp_hour = b.block_timestamp_hour
