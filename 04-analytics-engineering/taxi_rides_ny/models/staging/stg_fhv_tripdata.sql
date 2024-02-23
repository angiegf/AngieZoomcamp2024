{{
    config(
        materialized='view'
    )
}}

with tripdata as 
(
  select *
  from {{ source('staging','fhv_tripdata') }}
)
,
renamed_filtered as (select
    -- identifiers
    cast(dispatching_base_num as string) dispatching_base_num,
    cast(PUlocationID as integer) as pickup_locationid,
    cast(DOlocationID as integer )as dropoff_locationid,
    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp)  as dropoff_datetime,
    cast(coalesce(SR_Flag,0) as integer) as shared_ride_flag
from tripdata
where EXTRACT(YEAR FROM pickup_datetime) = 2019)

select distinct * from renamed_filtered


-- dbt build --select fact_trips --vars '{is_test_run: false}'
--{% if var('is_test_run', default=true) %} --if there's no value for is_test_run then use TRUE and by default add a limit 100, to get cheaper queries, great for production, 
--dbt build --select <model_name> --vars '{'is_test_run': 'false'}' to deploy

 -- limit 100

--{% endif %}