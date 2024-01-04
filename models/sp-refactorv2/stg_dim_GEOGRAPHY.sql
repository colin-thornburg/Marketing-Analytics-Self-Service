with 

source as (

    select * from {{ source('hub_load', 'dim_geography') }}

),

renamed as (

    select
        geography_id,
        geography_hierarchy_level_member_code

    from source

)

select * from renamed
