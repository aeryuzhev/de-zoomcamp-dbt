{{ config(materialized='table') }}

with 
green_data as (
    select
        *,
        'Green' as service_type
    from
        {{ ref('stg_green_tripdata') }}
),

yellow_data as (
    select
        *,
        'Yellow' as service_type
    from
        {{ ref('stg_yellow_tripdata') }}
),

trips_unioned as (
    select * 
    from green_data
    union all
    select * 
    from yellow_data
)

select 
    tu.tripid, 
    tu.vendorid, 
    tu.service_type,
    tu.ratecodeid, 
    tu.pickup_locationid, 
    pz.borough as pickup_borough, 
    pz.zone as pickup_zone, 
    tu.dropoff_locationid,
    dz.borough as dropoff_borough, 
    dz.zone as dropoff_zone,  
    tu.pickup_datetime, 
    tu.dropoff_datetime, 
    tu.store_and_fwd_flag, 
    tu.passenger_count, 
    tu.trip_distance, 
    tu.trip_type, 
    tu.fare_amount, 
    tu.extra, 
    tu.mta_tax, 
    tu.tip_amount, 
    tu.tolls_amount, 
    tu.ehail_fee, 
    tu.improvement_surcharge, 
    tu.total_amount, 
    tu.payment_type, 
    tu.payment_type_description, 
    tu.congestion_surcharge
from 
    trips_unioned tu
    join {{ ref('dim_zones') }} pz on tu.pickup_locationid = pz.locationid
    join {{ ref('dim_zones') }} dz on tu.dropoff_locationid = dz.locationid 