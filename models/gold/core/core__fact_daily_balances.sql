{{ config(
    materialized = 'incremental',
    unique_key = ['daily_balance_id'],
    incremental_strategy = 'merge',
    merge_exclude_columns = ['inserted_timestamp'],
    cluster_by = ['balance_date::DATE'],
    tags = ['scheduled_daily']
) }}

WITH date_spine AS (

    SELECT
        date_day AS balance_date
    FROM
        {{ source(
            'crosschain',
            'dim_dates'
        ) }}
    WHERE
        balance_date < SYSDATE() :: DATE

{% if is_incremental() %}
AND balance_date >= (
    SELECT
        MAX(balance_date)
    FROM
        {{ this }}
)
{% else %}
    AND balance_date >= '2024-01-01'
{% endif %}
),

{% if is_incremental() %}
latest_balances_from_table AS (
    SELECT
        account_id,
        balance_date,
        UPPER(asset_issuer) AS asset_issuer,
        UPPER(asset_code) AS asset_code,
        balance,
        is_deleted
    FROM
        {{ this }}
    WHERE
        balance_date = (
            SELECT
                MAX(balance_date)
            FROM
                {{ this }}
        )
),
{% endif %}

ver_tokens AS (
    SELECT
        UPPER(asset_issuer) AS asset_issuer,
        UPPER(asset_code) AS asset_code
    FROM
        {{ ref('price__ez_asset_metadata') }}
    WHERE
        is_verified
),
source_data AS (
    SELECT
        *
    FROM
        (
            SELECT
                account_id,
                closed_at,
                closed_at :: DATE AS snapshot_date,
                A.asset_issuer,
                A.asset_code,
                balance,
                deleted
            FROM
                {{ ref('core__fact_trust_lines') }} A
                JOIN ver_tokens b USING (
                    asset_issuer,
                    asset_code
                )
            WHERE
                closed_at IS NOT NULL

{% if is_incremental() %}
AND closed_at :: DATE > (
    SELECT
        MAX(balance_date)
    FROM
        {{ this }}
)
{% endif %}
UNION ALL
SELECT
    account_id,
    closed_at,
    closed_at :: DATE AS snapshot_date,
    'native' AS asset_issuer,
    'native' AS asset_code,
    balance,
    deleted
FROM
    {{ ref('core__fact_accounts') }}

{% if is_incremental() %}
WHERE
    closed_at :: DATE > (
        SELECT
            MAX(balance_date)
        FROM
            {{ this }}
    )
{% endif %}
) qualify ROW_NUMBER() over (
    PARTITION BY account_id,
    asset_issuer,
    asset_code,
    snapshot_date
    ORDER BY
        closed_at DESC
) = 1

{% if is_incremental() %}
UNION ALL
SELECT
    account_id,
    balance_date AS closed_at,
    balance_date AS snapshot_date,
    asset_issuer,
    asset_code,
    balance,
    is_deleted AS deleted,
FROM
    latest_balances_from_table
{% endif %}
),
account_asset_combinations AS (
    SELECT
        account_id,
        asset_issuer,
        asset_code,
        MIN(snapshot_date) AS first_seen_date
    FROM
        source_data
    GROUP BY
        account_id,
        asset_issuer,
        asset_code
),
daily_balances_with_gaps AS (
    SELECT
        d.balance_date,
        C.account_id,
        C.asset_issuer,
        C.asset_code,
        t.balance,
        t.deleted
    FROM
        date_spine d
        CROSS JOIN account_asset_combinations C
        LEFT JOIN source_data t
        ON C.account_id = t.account_id
        AND C.asset_issuer = t.asset_issuer
        AND C.asset_code = t.asset_code
        AND d.balance_date = t.snapshot_date
    WHERE
        d.balance_date >= C.first_seen_date
),
daily_balances_filled AS (
    SELECT
        balance_date,
        account_id,
        asset_issuer,
        asset_code,
        CASE
            WHEN LAST_VALUE(
                deleted ignore nulls
            ) over (
                PARTITION BY account_id,
                asset_issuer,
                asset_code
                ORDER BY
                    balance_date rows BETWEEN unbounded preceding
                    AND CURRENT ROW
            ) = TRUE THEN 0
            ELSE COALESCE(
                balance,
                LAST_VALUE(
                    balance ignore nulls
                ) over (
                    PARTITION BY account_id,
                    asset_issuer,
                    asset_code
                    ORDER BY
                        balance_date rows BETWEEN unbounded preceding
                        AND CURRENT ROW
                ),
                0
            )
        END AS balance,
        COALESCE(
            deleted,
            LAST_VALUE(
                deleted ignore nulls
            ) over (
                PARTITION BY account_id,
                asset_issuer,
                asset_code
                ORDER BY
                    balance_date rows BETWEEN unbounded preceding
                    AND CURRENT ROW
            ),
            FALSE
        ) AS is_deleted,
        CASE
            WHEN balance IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS balance_changed_on_date
    FROM
        daily_balances_with_gaps
)
SELECT
    balance_date,
    account_id,
    asset_issuer,
    asset_code,
    balance,
    is_deleted,
    balance_changed_on_date,
    balance - LAG(
        balance,
        1,
        0
    ) over (
        PARTITION BY account_id,
        asset_issuer,
        asset_code
        ORDER BY
            balance_date
    ) AS daily_balance_change,
    {{ dbt_utils.generate_surrogate_key(
        ['account_id','asset_issuer','asset_code','balance_date']
    ) }} AS daily_balance_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    daily_balances_filled
WHERE
    NOT (
        balance = 0
        AND is_deleted = TRUE
    )

{% if is_incremental() %}
AND balance_date > (
    SELECT
        MAX(balance_date)
    FROM
        {{ this }}
)
{% endif %}
