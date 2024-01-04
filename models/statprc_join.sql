with source_data as (
  select * from {{ ref('statprc') }}
),

price_types as (
  select * from (
    values 
      ('R', 'RETAIL'),
      ('C', 'CORE'),
      ('P', 'PROFESSIONAL')
  ) as t(PRICE_TYPE_CODE, PRICE_TYPE_CODE_DESCRIPTION)
),

unpivoted as (
  select
    CAST(source_data."STR" AS INTEGER) AS STORE_NUMBER,
    CAST(TRIM(source_data.LINE) AS VARCHAR(16)) AS LINE_CODE,
    CAST(TRIM(source_data."ITEM") AS VARCHAR(16)) AS ITEM_CODE,
    CASE
      WHEN price_types.PRICE_TYPE_CODE = 'R' THEN CAST(source_data.STUSER AS DECIMAL(7,2))
      WHEN price_types.PRICE_TYPE_CODE = 'C' THEN CAST(source_data.STCORE AS DECIMAL(7,2))
      WHEN price_types.PRICE_TYPE_CODE = 'P' THEN CAST(source_data.STPROFP AS DECIMAL(7,2))
    END AS PRICE,
    price_types.PRICE_TYPE_CODE,
    price_types.PRICE_TYPE_CODE_DESCRIPTION,
    CASE
      WHEN price_types.PRICE_TYPE_CODE = 'R' THEN source_data.USEREDAT
      WHEN price_types.PRICE_TYPE_CODE = 'C' THEN source_data.COREEDAT
      WHEN price_types.PRICE_TYPE_CODE = 'P' THEN source_data.PROFEDAT
    END AS PRICE_EFFECTIVE_DATE
  from source_data
  cross join price_types
)

select * from unpivoted