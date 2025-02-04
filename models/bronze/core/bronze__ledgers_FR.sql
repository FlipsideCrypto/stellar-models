{{ config (
    materialized = 'view'
) }}
{{ streamline_external_table_FR_query_v2(
    model = "history_ledgers",
    partition_function = "TRY_TO_DATE(left(split_part(split_part(file_name, '=', -1), '/', -1),8), 'YYYYMMDD')",
    partition_name = "partition_gte_id",
    unique_key = "SEQUENCE",
    other_cols = "partition_id, LEDGER_HASH, PREVIOUS_LEDGER_HASH, TRANSACTION_COUNT, OPERATION_COUNT, CLOSED_AT, ID, TOTAL_COINS, FEE_POOL, BASE_FEE, BASE_RESERVE, MAX_TX_SET_SIZE, PROTOCOL_VERSION, SUCCESSFUL_TRANSACTION_COUNT, FAILED_TRANSACTION_COUNT, TX_SET_OPERATION_COUNT, BATCH_ID, BATCH_RUN_DATE, BATCH_INSERT_TS, SOROBAN_FEE_WRITE_1KB, NODE_ID, SIGNATURE, TOTAL_BYTE_SIZE_OF_BUCKET_LIST"
) }}
--LEDGER_HEADER,
