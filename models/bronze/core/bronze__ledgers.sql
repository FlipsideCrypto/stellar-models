{{ config (
    materialized = 'view'
) }}
{{ streamline_external_table_query_v2(
    model = "history_ledgers",
    partition_function = " TO_DATE( SPLIT_PART(SPLIT_PART(file_name, '/', 3), '=', 2)|| '01', 'YYYYMMDD')",
    partition_name = "partition_id",
    unique_key = "SEQUENCE",
    other_cols = "LEDGER_HASH, PREVIOUS_LEDGER_HASH, TRANSACTION_COUNT, OPERATION_COUNT, CLOSED_AT, ID, TOTAL_COINS, FEE_POOL, BASE_FEE, BASE_RESERVE, MAX_TX_SET_SIZE, PROTOCOL_VERSION, LEDGER_HEADER, SUCCESSFUL_TRANSACTION_COUNT, FAILED_TRANSACTION_COUNT, TX_SET_OPERATION_COUNT, BATCH_ID, BATCH_RUN_DATE, BATCH_INSERT_TS, SOROBAN_FEE_WRITE_1KB, NODE_ID, SIGNATURE, TOTAL_BYTE_SIZE_OF_BUCKET_LIST"
) }}
