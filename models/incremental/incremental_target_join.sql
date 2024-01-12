{{
    config(
        materialized = 'incremental'
    )
}}

WITH source_data AS (
    SELECT
        S1.id,
        S1.name,
        S2.job,
        S2.years,
        GREATEST(S1.etl_load_date, S2.etl_load_date) AS etl_load_date
    FROM
        {{ ref('source_1') }} s1
    LEFT JOIN
        {{ ref('source_2') }} s2
    ON
        S1.id = S2.id
)

SELECT * FROM source_data

-- Logic to handle incremental updates

    {% if is_incremental() %}
        -- This condition helps in loading only new or updated records
       WHERE (source_data.etl_load_date >= (SELECT MAX(etl_load_date) FROM {{ this }}))
    {% endif %}
