{{ config(materialized='table')}}

select
    sf.tripid,
    sf.dispatching_base_num,
    sf.pickup_datetime,
    sf.dropOff_datetime,
    sf.PUlocationID,
    pz.borough as pickup_borough, 
    pz.zone as pickup_zone,
    sf.DOlocationID,
    dz.borough as dropoff_borough, 
    dz.zone as dropoff_zone,
    sf.SR_Flag,
    sf.Affiliated_base_number
from
    {{ ref('stg_fhv_tripdata') }} sf
    join {{ ref('dim_zones') }} pz on sf.PUlocationID = pz.locationid
    join {{ ref('dim_zones') }} dz on sf.DOlocationID = dz.locationid