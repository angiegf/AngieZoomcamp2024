{{ config(materialized="table") }}

with
    fhv_tripdata as (
        select
            -- identifiers
            dispatching_base_num,
            pickup_locationid,
            dropoff_locationid,
            pickup_datetime,
            dropoff_datetime,
            shared_ride_flag
        from {{ ref("stg_fhv_tripdata") }}

    ),

    dim_zones as (select * from {{ ref("dim_zones") }} where borough != 'Unknown')
select 
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    fhv_tripdata.dispatching_base_num,
    fhv_tripdata.pickup_locationid,
    fhv_tripdata.dropoff_locationid,
    fhv_tripdata.pickup_datetime,
    fhv_tripdata.dropoff_datetime,
    fhv_tripdata.shared_ride_flag
from fhv_tripdata
inner join
    dim_zones as pickup_zone
    on fhv_tripdata.pickup_locationid  = pickup_zone.locationid 
inner join
    dim_zones as dropoff_zone
    on fhv_tripdata.dropoff_locationid 
    = dropoff_zone.locationid 
