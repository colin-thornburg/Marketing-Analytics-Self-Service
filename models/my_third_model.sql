{{
    config(
        materialized='incremental',
        post_hook = "select 1 as id"
    )
}}

Select * from {{ ref('my_second_model') }}


{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  -- (uses > to include records whose timestamp occurred since the last run of this model)
  where id > (select max(id) from {{ this }})

{% endif %}