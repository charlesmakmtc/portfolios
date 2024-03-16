with
int_taxi_tripdata_revenue AS ( select * from {{ref('int_taxi_tripdata_revenue')}}),

dim_trip_revenue AS (
    select *
    from int_taxi_tripdata_revenue
)
select * from dim_trip_revenue