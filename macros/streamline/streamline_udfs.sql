{% macro create_udf_bulk_rest_api_v2() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_bulk_rest_api_v2(
        json OBJECT
    ) returns ARRAY {% if target.database == 'STELLAR' -%}
        api_integration = aws_stellar_api_prod_v2 AS 'https://qavdasgp43.execute-api.us-east-1.amazonaws.com/prod/udf_bulk_rest_api'
    {% else %}
        api_integration = aws_stellar_api_stg_v2 AS 'https://q75hks23yb.execute-api.us-east-1.amazonaws.com/stg/udf_bulk_rest_api'
    {%- endif %}
{% endmacro %}
