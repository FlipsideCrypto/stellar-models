{{ config(
    materialized = 'incremental',
    unique_key = 'test_timestamp',
    full_refresh = false,
    tags = ['observability']
) }}

WITH bq AS (

    SELECT
        SEQUENCE,
        block_timestamp,
        successful_transaction_count,
        failed_transaction_count,
        operation_count,
        tx_set_operation_count
    FROM
        {{ ref('core__fact_ledgers') }} A
    WHERE
        block_timestamp < DATEADD(
            HOUR,
            -24,
            SYSDATE()
        )

{% if is_incremental() %}
AND (
    block_timestamp >= DATEADD(
        HOUR,
        -96,(
            SELECT
                MAX(
                    max_block_timestamp
                )
            FROM
                {{ this }}
        )
    )
    OR ({% if var('OBSERV_FULL_TEST') %}
        1 = 1
    {% else %}
        SEQUENCE >= (
    SELECT
        MIN(VALUE) - 1
    FROM
        (
    SELECT
        ledgers_impacted_array
    FROM
        {{ this }}
        qualify ROW_NUMBER() over (
    ORDER BY
        test_timestamp DESC) = 1), LATERAL FLATTEN(input => ledgers_impacted_array))
    {% endif %})
)
{% endif %}
),
rpc AS (
    SELECT
        SEQUENCE,
        block_timestamp,
        successful_transaction_count,
        failed_transaction_count,
        operation_count,
        tx_set_operation_count
    FROM
        {{ ref('streamline__ledgers_complete') }} A
    WHERE
        block_timestamp < DATEADD(
            HOUR,
            -24,
            SYSDATE()
        )

{% if is_incremental() %}
AND (
    block_timestamp >= DATEADD(
        HOUR,
        -96,(
            SELECT
                MAX(
                    max_block_timestamp
                )
            FROM
                {{ this }}
        )
    )
    OR ({% if var('OBSERV_FULL_TEST') %}
        1 = 1
    {% else %}
        SEQUENCE >= (
    SELECT
        MIN(VALUE) - 1
    FROM
        (
    SELECT
        ledgers_impacted_array
    FROM
        {{ this }}
        qualify ROW_NUMBER() over (
    ORDER BY
        test_timestamp DESC) = 1), LATERAL FLATTEN(input => ledgers_impacted_array))
    {% endif %})
)
{% endif %}
)
SELECT
    'sequences' AS test_name,
    MIN(
        SEQUENCE
    ) AS min_SEQUENCE,
    MAX(
        SEQUENCE
    ) AS max_SEQUENCE,
    MIN(
        block_timestamp
    ) AS min_block_timestamp,
    MAX(
        block_timestamp
    ) AS max_block_timestamp,
    COUNT(1) AS ledgers_tested,
    COUNT(
        CASE
            WHEN bq.successful_transaction_count <> rpc.successful_transaction_count
            OR bq.successful_transaction_count IS NULL THEN SEQUENCE
        END
    ) AS ledgers_impacted_count,
    ARRAY_AGG(
        CASE
            WHEN bq.successful_transaction_count <> rpc.successful_transaction_count
            OR bq.successful_transaction_count IS NULL THEN SEQUENCE
        END
    ) within GROUP (
        ORDER BY
            SEQUENCE
    ) AS ledgers_impacted_array,
    SYSDATE() AS test_timestamp
FROM
    bq full
    OUTER JOIN rpc USING(
        SEQUENCE,
        block_timestamp
    )
