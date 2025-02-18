{{ config(
    materialized = 'view',
    tags = ['scheduled_daily']
) }}

SELECT
    blockchain,
    UPPER(address) AS address,
    creator,
    label_type,
    label_subtype,
    address_name,
    project_name,
    _is_deleted,
    labels_combined_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ source(
        'crosschain_silver',
        'labels_combined'
    ) }}
WHERE
    blockchain = 'stellar'
