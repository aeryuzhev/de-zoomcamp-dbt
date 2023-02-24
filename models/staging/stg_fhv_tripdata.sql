{{ config(materliazed='view' )}}

select
    {{ dbt_utils.surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,
    cast(dispatching_base_num as string) as dispatching_base_num,
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropOff_datetime,
    cast(PUlocationID as integer) as PUlocationID,
    cast(DOlocationID as integer) as DOlocationID,
    cast(SR_Flag as integer) as SR_Flag,
    cast(Affiliated_base_number as string) as Affiliated_base_number
from
    {{ source('staging', 'fhv_tripdata') }}

{% if var('is_test_run', default=false) %}
    limit 100
{% endif %}