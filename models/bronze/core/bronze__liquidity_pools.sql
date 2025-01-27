{{ config (
    materialized = 'view'
) }}
{{ streamline_external_table_query_v2(
    model = "liquidity_pools",
    partition_function = " TO_DATE( SPLIT_PART(SPLIT_PART(file_name, '/', 3), '=', 2)|| '01', 'YYYYMMDD')",
    partition_name = "partition_id",
    unique_key = "LIQUIDITY_POOL_ID",
    other_cols = "TYPE, FEE, TRUSTLINE_COUNT, POOL_SHARE_COUNT, ASSET_A_TYPE, ASSET_A_CODE, ASSET_A_ISSUER, ASSET_A_ID, ASSET_A_AMOUNT, ASSET_B_TYPE, ASSET_B_CODE, ASSET_B_ISSUER, ASSET_B_ID, ASSET_B_AMOUNT, LAST_MODIFIED_LEDGER, LEDGER_ENTRY_CHANGE, DELETED, BATCH_ID, BATCH_RUN_DATE, BATCH_INSERT_TS, CLOSED_AT, LEDGER_SEQUENCE"
) }}
