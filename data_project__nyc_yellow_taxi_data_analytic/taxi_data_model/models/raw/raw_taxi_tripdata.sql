with
raw__yellow_tripdata AS (
        select
            {{ dbt_utils.star(source('taxi_data', 'yellow_tripdata')) }}
        from {{ source('taxi_data', 'yellow_tripdata')}}
    )
select * 
from raw__yellow_tripdata
where tpep_dropoff_datetime <= '2020-03-01 23:59:59'