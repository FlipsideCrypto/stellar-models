{{ config (
    materialized = "ephemeral",
    unique_key = "closed_at",
) }}

WITH base AS (

    SELECT
        closed_at :: DATE AS closed_at,
        MAX(sequence) AS sequence
    FROM
        {{ ref("silver__ledgers") }}
    GROUP BY
        closed_at :: DATE
)
SELECT
    closed_at,
    sequence
FROM
    base
WHERE
    closed_at <> (
        SELECT
            MAX(closed_at)
        FROM
            base
    )
