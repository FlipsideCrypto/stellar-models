-- depends_on: {{ ref('silver__operations') }}
{{ config(
    materialized = 'incremental',
    unique_key = ["id"],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE','closed_at::DATE','type_string'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(source_account,asset,asset_code,asset_issuer,buying_asset_code,buying_asset_issuer,selling_asset_code,selling_asset_issuer);",
    tags = ['scheduled_core']
) }}

SELECT
    id,
    closed_at,
    closed_at AS block_timestamp,
    account,
    amount,
    asset,
    asset_code,
    asset_issuer,
    asset_type,
    asset_id,
    authorize,
    balance_id,
    buying_asset_code,
    buying_asset_issuer,
    buying_asset_type,
    buying_asset_id,
    "from" AS from_account,
    funder,
    high_threshold,
    home_domain,
    inflation_dest,
    "into" into_account,
    "limit" AS limit_amount,
    low_threshold,
    master_key_weight,
    med_threshold,
    NAME,
    offer_id,
    path,
    price,
    price_r,
    selling_asset_code,
    selling_asset_issuer,
    selling_asset_type,
    selling_asset_id,
    set_flags,
    set_flags_s,
    signer_account_id,
    signer_key,
    signer_weight,
    source_amount,
    source_asset_code,
    source_asset_issuer,
    source_asset_type,
    source_asset_id,
    source_max,
    starting_balance,
    "to" AS to_account,
    trustee,
    trustor,
    trustline_asset,
    VALUE,
    clear_flags,
    clear_flags_s,
    destination_min,
    bump_to,
    sponsor,
    sponsored_id,
    begin_sponsor,
    authorize_to_maintain_liabilities,
    clawback_enabled,
    liquidity_pool_id,
    reserve_a_asset_type,
    reserve_a_asset_id,
    reserve_a_asset_code,
    reserve_a_asset_issuer,
    reserve_a_max_amount,
    reserve_a_deposit_amount,
    reserve_b_asset_type,
    reserve_b_asset_id,
    reserve_b_asset_code,
    reserve_b_asset_issuer,
    reserve_b_max_amount,
    reserve_b_deposit_amount,
    min_price,
    min_price_r,
    max_price,
    max_price_r,
    shares_received,
    reserve_a_min_amount,
    reserve_b_min_amount,
    shares,
    reserve_a_withdraw_amount,
    reserve_b_withdraw_amount,
    source_account,
    op_source_account_muxed,
    transaction_id,
    TYPE,
    type_string,
    ledger_sequence,
    op_account_muxed,
    op_account_muxed_id,
    ledger_key_hash,
    batch_id,
    batch_run_date,
    batch_insert_ts,
    asset_balance_changes,
    PARAMETERS,
    parameters_decoded,
    FUNCTION,
    address,
    soroban_operation_type,
    extend_to,
    contract_id,
    contract_code_hash,
    operation_result_code,
    operation_trace_code,
    begin_sponsor_muxed,
    begin_sponsor_muxed_id,
    claimable_balance_id,
    claimant,
    claimants,
    claimant_muxed,
    claimant_muxed_id,
    data_account_id,
    data_name,
    details_json,
    from_muxed,
    from_muxed_id,
    funder_muxed,
    funder_muxed_id,
    into_muxed,
    into_muxed_id,
    ledgers_to_expire,
    op_account_id,
    to_muxed,
    to_muxed_id,
    trustee_muxed,
    trustee_muxed_id,
    trustline_account_id,
    trustor_muxed,
    trustor_muxed_id,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['id']) }} AS fact_operations_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('silver__operations') }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
