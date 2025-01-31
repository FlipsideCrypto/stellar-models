{{ config (
    materialized = 'view'
) }}
{{ streamline_external_table_FR_query_v2(
    model = "history_assets",
    partition_function = " TO_DATE( SPLIT_PART(SPLIT_PART(file_name, '/', 3), '=', 2)|| '01', 'YYYYMMDD')",
    partition_name = "partition_id",
    unique_key = "ID",
    other_cols = " ASSET_TYPE, ASSET_CODE, ASSET_ISSUER, BATCH_RUN_DATE, BATCH_ID, BATCH_INSERT_TS, ASSET_ID"
) }}
