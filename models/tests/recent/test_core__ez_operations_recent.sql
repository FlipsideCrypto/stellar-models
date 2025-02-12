{{ config (
    materialized = 'view',
    tags = ['recent_test']
) }}

WITH last_3_days AS (

    SELECT
        closed_at
    FROM
        {{ ref("_max_ledger_by_date") }}
        qualify ROW_NUMBER() over (
            ORDER BY
                closed_at DESC
        ) = 3
)
SELECT
    *
FROM
    {{ ref('core__ez_operations') }}
WHERE
    closed_at :: DATE >= (
        SELECT
            closed_at
        FROM
            last_3_days
    )
