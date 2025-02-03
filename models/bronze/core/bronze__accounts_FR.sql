{{ config (
    materialized = 'view'
) }}
{{ streamline_external_table_FR_query_v2(
    model = "accounts",
    partition_function = "TRY_TO_DATE(left(split_part(split_part(file_name, '=', -1), '/', -1),8), 'YYYYMMDD')",
    partition_name = "partition_gte_id",
    unique_key = "ACCOUNT_ID",
    other_cols = "partition_id, BALANCE, BUYING_LIABILITIES, SELLING_LIABILITIES, SEQUENCE_NUMBER, NUM_SUBENTRIES, INFLATION_DESTINATION, FLAGS, HOME_DOMAIN, MASTER_WEIGHT, THRESHOLD_LOW, THRESHOLD_MEDIUM, THRESHOLD_HIGH, LAST_MODIFIED_LEDGER, LEDGER_ENTRY_CHANGE, DELETED, BATCH_ID, BATCH_RUN_DATE, BATCH_INSERT_TS, SPONSOR, NUM_SPONSORED, NUM_SPONSORING, SEQUENCE_LEDGER, SEQUENCE_TIME, CLOSED_AT, LEDGER_SEQUENCE"
) }}
