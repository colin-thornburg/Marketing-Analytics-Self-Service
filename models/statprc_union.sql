
with unpivoted as (
  select
    STR as STORE_NUMBER,
    LINE as LINE_CODE,
    ITEM as ITEM_CODE,
    STUSER as PRICE,
    'R' as PRICE_TYPE_CODE,
    USEREDAT as PRICE_EFFECTIVE_DATE
  from {{ ref('statprc') }}

  union all

  select
    STR as STORE_NUMBER,
    LINE as LINE_CODE,
    ITEM as ITEM_CODE,
    STCORE as PRICE,
    'C' as PRICE_TYPE_CODE,
    COREEDAT as PRICE_EFFECTIVE_DATE
  from {{ ref('statprc') }}

  union all

  select
    STR as STORE_NUMBER,
    LINE as LINE_CODE,
    ITEM as ITEM_CODE,
    STPROFP as PRICE,
    'P' as PRICE_TYPE_CODE,
    PROFEDAT as PRICE_EFFECTIVE_DATE
  from {{ ref('statprc') }}
)

select * from unpivoted