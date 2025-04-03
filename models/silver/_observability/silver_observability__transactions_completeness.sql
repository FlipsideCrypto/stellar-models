{{ config(
    materialized = 'incremental',
    unique_key = 'test_timestamp',
    full_refresh = false,
    tags = ['observability']
) }}

WITH summary_stats AS (

    SELECT
        MIN(SEQUENCE) AS min_SEQUENCE,
        MAX(SEQUENCE) AS max_SEQUENCE,
        MIN(block_timestamp) AS min_block_timestamp,
        MAX(block_timestamp) AS max_block_timestamp,
        COUNT(1) AS ledgers_tested
    FROM
        {{ ref('core__fact_ledgers') }}
    WHERE
        block_timestamp <= DATEADD('hour', -12, CURRENT_TIMESTAMP())

{% if is_incremental() %}
AND (
    SEQUENCE >= (
        SELECT
            MIN(SEQUENCE)
        FROM
            (
                SELECT
                    MIN(SEQUENCE) AS SEQUENCE
                FROM
                    {{ ref('core__fact_ledgers') }}
                WHERE
                    block_timestamp BETWEEN DATEADD('hour', -96, CURRENT_TIMESTAMP())
                    AND DATEADD('hour', -95, CURRENT_TIMESTAMP())
                UNION
                SELECT
                    MIN(VALUE) - 1 AS SEQUENCE
                FROM
                    (
                        SELECT
                            ledgers_impacted_array
                        FROM
                            {{ this }}
                            qualify ROW_NUMBER() over (
                                ORDER BY
                                    test_timestamp DESC
                            ) = 1
                    ),
                    LATERAL FLATTEN(
                        input => ledgers_impacted_array
                    )
            )
    ) {% if var('OBSERV_FULL_TEST') %}
        OR SEQUENCE >= 0
    {% endif %}
)
{% endif %}
),
base_sequence AS (
    SELECT
        SEQUENCE,
        successful_transaction_count + failed_transaction_count AS transaction_count
    FROM
        {{ ref('core__fact_ledgers') }}
    WHERE
        SEQUENCE BETWEEN (
            SELECT
                min_SEQUENCE
            FROM
                summary_stats
        )
        AND (
            SELECT
                max_SEQUENCE
            FROM
                summary_stats
        )
),
actual_tx_counts AS (
    SELECT
        ledger_SEQUENCE AS SEQUENCE,
        COUNT(1) AS transaction_count
    FROM
        {{ ref('core__fact_transactions') }}
    WHERE
        SEQUENCE BETWEEN (
            SELECT
                min_SEQUENCE
            FROM
                summary_stats
        )
        AND (
            SELECT
                max_SEQUENCE
            FROM
                summary_stats
        )
    GROUP BY
        SEQUENCE
),
potential_missing_txs AS (
    SELECT
        e.sequence
    FROM
        base_sequence e
        LEFT OUTER JOIN actual_tx_counts A
        ON e.sequence = A.sequence
    WHERE
        COALESCE(
            A.transaction_count,
            0
        ) <> e.transaction_count
),
impacted_seqs AS (
    SELECT
        COUNT(1) AS ledgers_impacted_count,
        ARRAY_AGG(SEQUENCE) within GROUP (
            ORDER BY
                SEQUENCE
        ) AS ledgers_impacted_array
    FROM
        potential_missing_txs
)
SELECT
    'transactions' AS test_name,
    min_sequence,
    max_sequence,
    min_block_timestamp,
    max_block_timestamp,
    ledgers_tested,
    ledgers_impacted_count,
    ledgers_impacted_array,
    SYSDATE() AS test_timestamp
FROM
    summary_stats
    JOIN impacted_seqs
    ON 1 = 1
