-- depends_on: {{ ref('bronze__operations') }}
{{ config(
    materialized = 'incremental',
    unique_key = "id",
    incremental_predicates = ["dynamic_range_predicate","partition_id::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['closed_at::DATE','partition_id','modified_timestamp::DATE'],
    tags = ['scheduled_core'],
) }}

{% if execute %}

{% if is_incremental() %}
{% set max_inserted_query %}

SELECT
    MAX(batch_insert_ts) AS batch_insert_ts
FROM
    {{ this }}

    {% endset %}
    {% set max_inserted_timestamp = run_query(max_inserted_query) [0] [0] %}
{% endif %}

{% if is_incremental() %}
{% set max_part_query %}
SELECT
    MAX(partition_id) AS partition_id
FROM
    {{ this }}

    {% endset %}
    {% set max_part = run_query(max_part_query) [0] [0] %}
{% endif %}
{% endif %}

WITH pre_final AS (
    SELECT
        partition_id AS partition_id,
        id :: INTEGER AS id,
        VALUE :source_account :: STRING AS source_account,
        VALUE :source_account_muxed :: STRING AS op_source_account_muxed,
        VALUE :ledger_sequence :: INTEGER AS ledger_sequence,
        VALUE :transaction_id :: INTEGER AS transaction_id,
        VALUE :type :: INTEGER AS TYPE,
        VALUE :type_string :: STRING AS type_string,
        VALUE :details :account :: STRING AS account,
        VALUE :details :account_muxed :: STRING AS op_account_muxed,
        VALUE :details :account_muxed_id :: INTEGER AS op_account_muxed_id,
        VALUE :details :account_id :: STRING AS op_account_id,
        VALUE :details :amount :: FLOAT AS amount,
        VALUE :details :asset :: STRING AS asset,
        VALUE :details :asset_code :: STRING AS asset_code,
        VALUE :details :asset_issuer :: STRING AS asset_issuer,
        VALUE :details :asset_id :: STRING AS asset_id,
        VALUE :details :asset_type :: STRING AS asset_type,
        VALUE :details :authorize :: BOOLEAN AS authorize,
        VALUE :details :balance_id :: STRING AS balance_id,
        VALUE :details :buying_asset_code :: STRING AS buying_asset_code,
        VALUE :details :buying_asset_issuer :: STRING AS buying_asset_issuer,
        VALUE :details :buying_asset_id :: STRING AS buying_asset_id,
        VALUE :details :buying_asset_type :: STRING AS buying_asset_type,
        VALUE :details :claimable_balance_id :: STRING AS claimable_balance_id,
        VALUE :details :claimant :: STRING AS claimant,
        VALUE :details :claimant_muxed :: STRING AS claimant_muxed,
        VALUE :details :claimant_muxed_id :: INTEGER AS claimant_muxed_id,
        VALUE :details :claimants :: variant AS claimants,
        VALUE :details :data_account_id :: STRING AS data_account_id,
        VALUE :details :data_name :: STRING AS data_name,
        VALUE :details :"from" :: STRING AS "from",
        VALUE :details :from_muxed :: STRING AS from_muxed,
        VALUE :details :from_muxed_id :: INTEGER AS from_muxed_id,
        VALUE :details :funder :: STRING AS funder,
        VALUE :details :funder_muxed :: STRING AS funder_muxed,
        VALUE :details :funder_muxed_id :: INTEGER AS funder_muxed_id,
        VALUE :details :high_threshold :: INTEGER AS high_threshold,
        VALUE :details :home_domain :: STRING AS home_domain,
        VALUE :details :inflation_dest :: STRING AS inflation_dest,
        VALUE :details :"into" :: STRING AS "into",
        VALUE :details :into_muxed :: STRING AS into_muxed,
        VALUE :details :into_muxed_id :: INTEGER AS into_muxed_id,
        VALUE :details :"limit" :: FLOAT AS "limit",
        VALUE :details :low_threshold :: INTEGER AS low_threshold,
        VALUE :details :master_key_weight :: INTEGER AS master_key_weight,
        VALUE :details :med_threshold :: INTEGER AS med_threshold,
        VALUE :details :name :: STRING AS NAME,
        VALUE :details :offer_id :: INTEGER AS offer_id,
        VALUE :details :path :: variant AS path,
        VALUE :details :price :: ARRAY AS price,
        VALUE :details :price_r :: variant AS price_r,
        VALUE :details :selling_asset_code :: STRING AS selling_asset_code,
        VALUE :details :selling_asset_issuer :: STRING AS selling_asset_issuer,
        VALUE :details :selling_asset_id :: STRING AS selling_asset_id,
        VALUE :details :selling_asset_type :: STRING AS selling_asset_type,
        VALUE :details :set_flags :: ARRAY AS set_flags,
        VALUE :details :set_flags_s :: ARRAY AS set_flags_s,
        VALUE :details :signer_account_id :: STRING AS signer_account_id,
        VALUE :details :signer_key :: STRING AS signer_key,
        VALUE :details :signer_weight :: INTEGER AS signer_weight,
        VALUE :details :source_amount :: FLOAT AS source_amount,
        VALUE :details :source_asset_code :: STRING AS source_asset_code,
        VALUE :details :source_asset_issuer :: STRING AS source_asset_issuer,
        VALUE :details :source_asset_id :: STRING AS source_asset_id,
        VALUE :details :source_asset_type :: STRING AS source_asset_type,
        VALUE :details :source_max :: FLOAT AS source_max,
        VALUE :details :starting_balance :: FLOAT AS starting_balance,
        VALUE :details :"to" :: STRING AS "to",
        VALUE :details :to_muxed :: STRING AS to_muxed,
        VALUE :details :to_muxed_id :: INTEGER AS to_muxed_id,
        VALUE :details :trustee :: STRING AS trustee,
        VALUE :details :trustee_muxed :: STRING AS trustee_muxed,
        VALUE :details :trustee_muxed_id :: INTEGER AS trustee_muxed_id,
        VALUE :details :trustline_account_id :: STRING AS trustline_account_id,
        VALUE :details :trustline_asset :: STRING AS trustline_asset,
        VALUE :details :trustor :: STRING AS trustor,
        VALUE :details :trustor_muxed :: STRING AS trustor_muxed,
        VALUE :details :trustor_muxed_id :: INTEGER AS trustor_muxed_id,
        VALUE :details :value :: STRING AS VALUE,
        VALUE :details :clear_flags :: ARRAY AS clear_flags,
        VALUE :details :clear_flags_s :: ARRAY AS clear_flags_s,
        VALUE :details :destination_min :: STRING AS destination_min,
        VALUE :details :bump_to :: STRING AS bump_to,
        VALUE :details :authorize_to_maintain_liabilities :: BOOLEAN AS authorize_to_maintain_liabilities,
        VALUE :details :clawback_enabled :: BOOLEAN AS clawback_enabled,
        VALUE :details :sponsor :: STRING AS sponsor,
        VALUE :details :sponsored_id :: STRING AS sponsored_id,
        VALUE :details :begin_sponsor :: STRING AS begin_sponsor,
        VALUE :details :begin_sponsor_muxed :: STRING AS begin_sponsor_muxed,
        VALUE :details :begin_sponsor_muxed_id :: INTEGER AS begin_sponsor_muxed_id,
        VALUE :details :liquidity_pool_id :: STRING AS liquidity_pool_id,
        VALUE :details :reserve_a_asset_type :: STRING AS reserve_a_asset_type,
        VALUE :details :reserve_a_asset_code :: STRING AS reserve_a_asset_code,
        VALUE :details :reserve_a_asset_issuer :: STRING AS reserve_a_asset_issuer,
        VALUE :details :reserve_a_asset_id :: STRING AS reserve_a_asset_id,
        VALUE :details :reserve_a_max_amount :: FLOAT AS reserve_a_max_amount,
        VALUE :details :reserve_a_deposit_amount :: FLOAT AS reserve_a_deposit_amount,
        VALUE :details :reserve_b_asset_type :: STRING AS reserve_b_asset_type,
        VALUE :details :reserve_b_asset_code :: STRING AS reserve_b_asset_code,
        VALUE :details :reserve_b_asset_issuer :: STRING AS reserve_b_asset_issuer,
        VALUE :details :reserve_b_asset_id :: STRING AS reserve_b_asset_id,
        VALUE :details :reserve_b_max_amount :: FLOAT AS reserve_b_max_amount,
        VALUE :details :reserve_b_deposit_amount :: FLOAT AS reserve_b_deposit_amount,
        VALUE :details :min_price :: FLOAT AS min_price,
        VALUE :details :min_price_r :: variant AS min_price_r,
        VALUE :details :max_price :: FLOAT AS max_price,
        VALUE :details :max_price_r :: variant AS max_price_r,
        VALUE :details :shares_received :: FLOAT AS shares_received,
        VALUE :details :reserve_a_min_amount :: FLOAT AS reserve_a_min_amount,
        VALUE :details :reserve_a_withdraw_amount :: FLOAT AS reserve_a_withdraw_amount,
        VALUE :details :reserve_b_min_amount :: FLOAT AS reserve_b_min_amount,
        VALUE :details :reserve_b_withdraw_amount :: FLOAT AS reserve_b_withdraw_amount,
        VALUE :details :shares :: FLOAT AS shares,
        VALUE :details :asset_balance_changes :: variant AS asset_balance_changes,
        VALUE :details :parameters :: variant AS PARAMETERS,
        VALUE :details :parameters_decoded :: variant AS parameters_decoded,
        VALUE :details :function :: STRING AS FUNCTION,
        VALUE :details :address :: STRING AS address,
        VALUE :details :type :: STRING AS soroban_operation_type,
        VALUE :details :extend_to :: INTEGER AS extend_to,
        VALUE :details :contract_id :: STRING AS contract_id,
        VALUE :details :contract_code_hash :: STRING AS contract_code_hash,
        VALUE :details :ledger_key_hash :: STRING AS ledger_key_hash,
        VALUE :details :ledgers_to_expire :: INTEGER AS ledgers_to_expire,
        VALUE :details :: variant AS details_json,
        VALUE :operation_result_code :: STRING AS operation_result_code,
        VALUE :operation_trace_code :: STRING AS operation_trace_code,
        TO_TIMESTAMP(
            VALUE :closed_at :: INT,
            6
        ) AS closed_at,
        VALUE :batch_id :: STRING AS batch_id,
        VALUE :batch_run_date :: TIMESTAMP AS batch_run_date,
        VALUE :batch_insert_ts :: INT AS batch_insert_ts,
        NULL AS _inserted_timestamp
    FROM
        {{ source(
            'bronze_streamline',
            'history_operations'
        ) }}
        {# {% if is_incremental() %}
        {{ ref('bronze__operations') }}
    {% else %}
        {{ ref('bronze__operations_FR') }}
    {% endif %}

    #}

{% if is_incremental() %}
WHERE
    partition_id >= '{{ max_part }}'
    AND _inserted_timestamp > '{{ max_batch }}'
{% endif %}

qualify ROW_NUMBER() over (
    PARTITION BY id
    ORDER BY
        batch_insert_ts DESC
) = 1
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['id']
    ) }} AS operations_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    pre_final
