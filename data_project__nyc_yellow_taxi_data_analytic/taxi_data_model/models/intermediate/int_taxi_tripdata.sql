
with
stg_taxi_tripdata AS ( select * from {{ref('stg_taxi_tripdata')}}),


int_taxi_tripdata AS (
    select
        row_index_id,
        row_created_at,
        row_updated_at,
        --- taxi company
        vendor_id,
        --- business start / end / distance     (4/ dim_trip_distance_and_location.sql)
        tpep_pickup_datetime, 
        tpep_dropoff_datetime, 
        ----  trip date features
        extract(year from tpep_pickup_datetime)   as trip_start_year,
        extract(month from tpep_pickup_datetime)  as trip_start_month,
        date(tpep_pickup_datetime)                as trip_start_date,
        extract(year from tpep_dropoff_datetime)  as trip_end_year,
        extract(month from tpep_dropoff_datetime) as trip_end_month,
        date(tpep_dropoff_datetime)               as trip_end_date,
        pu_location_id, 
        do_location_id, 
        trip_distance, 
        rate_code_id,
        store_and_fwd_flag, 
        --- payment
        payment_type,
        --- revenue
        fare_amount, 
        extra, 
        mta_tax, 
        tip_amount, 
        tolls_amount, 
        improvement_surcharge, 
        total_amount, 
        congestion_surcharge, 
        airport_fee
    from stg_taxi_tripdata
)

select * from int_taxi_tripdata
