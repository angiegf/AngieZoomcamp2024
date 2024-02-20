{{
    config(
        materialized='view'
    )
}}

with tripdata as 
(
  select *,
    row_number() over(partition by dispatching_base_num, pickup_datetime) as rn --making sure we have no duplicates
  from {{ source('staging','fhv_tripdata') }}
)
select
    -- identifiers
    {{ dbt_utils.generate_surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid, -- as each taxi can only take one ride at the same time 
    {{ dbt.safe_cast("dispatching_base_num", api.Column.translate_type("string")) }} as dispatching_base_num,
    {{ dbt.safe_cast("Affiliated_base_number", api.Column.translate_type("string")) }} as affiliated_base_number,
    {{ dbt.safe_cast("PUlocationID", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("DOlocationID", api.Column.translate_type("integer")) }} as dropoff_locationid,
    
    -- timestamps
    TIMESTAMP_MILLIS(CASt(pickup_datetime/1000000 as INT)) as pickup_datetime,
    TIMESTAMP_MILLIS(CASt(pickup_datetime/1000000 as INT)) as dropoff_datetime,
    cast(coalesce(SR_Flag,0) as integer) as shared_ride_flag
from tripdata
where rn = 1


-- dbt build --select fact_trips --vars '{is_test_run: false}'
--{% if var('is_test_run', default=true) %} --if there's no value for is_test_run then use TRUE and by default add a limit 100, to get cheaper queries, great for production, 
--dbt build --select <model_name> --vars '{'is_test_run': 'false'}' to deploy

 -- limit 100

--{% endif %}