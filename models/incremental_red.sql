{{
   config(
       materialized='incremental',
       unique_key='id'
   )
}}

-- get the timestamp for the last run time
-- changes at must take a string literal, not a sql query
{% if is_incremental() %}
   {% set max_ts = run_query("select max(last_run_ts) from " ~ this) %}

   {% if execute %}
       {% set max_ts = max_ts.columns[0].values()[0] %}
   {% else %}
       {% set max_ts = [] %}
   {% endif %}
{% endif %}

select *, (select current_timestamp()) as last_run_ts
--exclude (metadata$action, metadata$isupdate, metadata$row_id)
from {{ source('changes_source', 't1') }}

{% if is_incremental() %}
-- updates since last run timestamp
changes(information => default)
at(timestamp => '{{ max_ts }}'::timestamp_tz)
where metadata$action = 'INSERT'
{% endif %}