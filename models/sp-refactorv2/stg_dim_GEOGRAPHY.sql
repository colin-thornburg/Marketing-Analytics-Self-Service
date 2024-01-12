  select
        geography_id,
        geography_hierarchy_level_member_code

    from {{ source('hub_load', 'dim_geography') }}