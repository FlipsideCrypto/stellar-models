{{ config(
    materialized = 'incremental',
    unique_key = 'test_timestamp',
    full_refresh = false,
    tags = ['observability']
) }}

WITH source AS (

    SELECT
        SEQUENCE,
        block_timestamp,
        LAG(
            SEQUENCE,
            1
        ) over (
            ORDER BY
                SEQUENCE ASC
        ) AS prev_SEQUENCE
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
seq_gen AS (
    SELECT
        _id AS SEQUENCE
    FROM
        {{ source(
            'crosschain_silver',
            'number_sequence'
        ) }}
    WHERE
        _id BETWEEN (
            SELECT
                MIN(SEQUENCE)
            FROM
                source
        )
        AND (
            SELECT
                MAX(SEQUENCE)
            FROM
                source
        )
)
SELECT
    'sequences' AS test_name,
    MIN(
        b.sequence
    ) AS min_SEQUENCE,
    MAX(
        b.sequence
    ) AS max_SEQUENCE,
    MIN(
        b.block_timestamp
    ) AS min_block_timestamp,
    MAX(
        b.block_timestamp
    ) AS max_block_timestamp,
    COUNT(1) AS ledgers_tested,
    COUNT(
        CASE
            WHEN C.sequence IS NOT NULL THEN A.sequence
        END
    ) AS ledgers_impacted_count,
    ARRAY_AGG(
        CASE
            WHEN C.sequence IS NOT NULL THEN A.sequence
        END
    ) within GROUP (
        ORDER BY
            A.sequence
    ) AS ledgers_impacted_array,
    ARRAY_AGG(
        DISTINCT CASE
            WHEN C.sequence IS NOT NULL THEN OBJECT_CONSTRUCT(
                'prev_sequence',
                C.prev_SEQUENCE,
                'SEQUENCE',
                C.sequence
            )
        END
    ) AS test_failure_details,
    SYSDATE() AS test_timestamp
FROM
    seq_gen A
    LEFT JOIN source b
    ON A.sequence = b.sequence
    LEFT JOIN source C
    ON A.sequence > C.prev_SEQUENCE
    AND A.sequence < C.sequence
    AND C.sequence - C.prev_SEQUENCE <> 1
WHERE
    COALESCE(
        b.prev_SEQUENCE,
        C.prev_SEQUENCE
    ) IS NOT NULL
