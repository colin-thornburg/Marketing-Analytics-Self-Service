with 

source as (

    select CCUST as "CCUST#"
    ,CUSTOMER from {{ source('edw_staging', 'CUR_CUS_CMASTER') }}

),

renamed as (

    select *

    from source

)

select * from renamed
