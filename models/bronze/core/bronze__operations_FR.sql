{{ config (
    materialized = 'view'
) }}
{{ streamline_external_table_FR_query_v2(
    model = "history_operations",
    partition_function = "TRY_TO_DATE(left(split_part(split_part(file_name, '=', -1), '/', -1),8), 'YYYYMMDD')",
    partition_name = "partition_gte_id",
    unique_key = "ID",
    other_cols = 'partition_id,VALUE'
) }}
