with 

source as (

    select * from {{ source('edw_staging', 'CUR_CUS_DBARCUD') }}

),

renamed as (

    select
        addr2,
        addr1,
        suite,
        city,
        zip,
        pstl_cd_ext,
        state,
        country_code,
        last_update_date

    from source

)

select * from renamed
