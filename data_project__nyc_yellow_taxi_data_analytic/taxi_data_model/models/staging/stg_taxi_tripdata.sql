
with
raw_taxi_tripdata AS ( select * from {{ref('raw_taxi_tripdata')}}),

stg_taxi_tripdata AS (
    select
        --- row based data
        row_number() OVER () AS row_index_id,
        'A' as row_status, -- A = active, D = deleted
        now() as row_created_at,
        now() as row_updated_at,

        --- raw data
        "VendorID"      as vendor_id,
        tpep_pickup_datetime, 
        tpep_dropoff_datetime, 
        passenger_count, 
        trip_distance, 
        "RatecodeID"    as rate_code_id, 
        store_and_fwd_flag, 
        "PULocationID"  as pu_location_id, 
        "DOLocationID"  as do_location_id, 
        payment_type, 
        fare_amount, 
        extra, 
        mta_tax, 
        tip_amount, 
        tolls_amount, 
        improvement_surcharge, 
        total_amount, 
        congestion_surcharge, 
        airport_fee

    from raw_taxi_tripdata
    where date(tpep_pickup_datetime) >= '2020-01-01'
)

select * from stg_taxi_tripdata