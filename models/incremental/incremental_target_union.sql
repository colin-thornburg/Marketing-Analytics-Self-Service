{{
    config(
        materialized = 'incremental',
        unique_key = 'id'
    )
}}

{% set source_1_relation = ref('source_1') %}
{% set source_2_relation = ref('source_2') %}

{{ dbt_utils.union_relations(
    relations=[source_1_relation, source_2_relation],
    source_column_name="_dbt_source_relation"
) }}

-- Logic to handle incremental updates
{% if is_incremental() %}
    WHERE etl_load_date > (SELECT MAX(etl_load_date) FROM {{ this }})
{% endif %}
