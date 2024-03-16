
with
int_taxi_tripdata AS ( select * from {{ref('int_taxi_tripdata')}}),


int_taxi_tripdata_intrvl__feat_ext AS (
    select
        row_index_id
        ,tpep_pickup_datetime
        ,tpep_dropoff_datetime
        ---- trip time-based features extraction
        ,(tpep_dropoff_datetime - tpep_pickup_datetime)                      as total_trip_time
        ,EXTRACT(minute from (tpep_dropoff_datetime - tpep_pickup_datetime)) as total_trip_time_in_minutes
        ---- trip date-based features extraction
        ,EXTRACT(dow from tpep_pickup_datetime)     as trip_start_weekday
        ,TO_CHAR(tpep_pickup_datetime, 'day')       as trip_start_weekday_name
        ,EXTRACT(dow from tpep_dropoff_datetime)    as trip_end_weekday
        ,TO_CHAR(tpep_dropoff_datetime, 'day')      as trip_end_weekday_name
    from int_taxi_tripdata
)

,int_taxi_tripdata_intrvl AS (
    select 
        *
        ---- labeling weekday or weekend for trip dates
        ,case 
            when trip_start_weekday in (0, 6) then 'weekend'
            else 'weekday'
        end as trip_start_date_weekday_or_weekend
        ,case 
            when trip_end_weekday in (0, 6) then 'weekend'
            else 'weekday'
        end as trip_end_date_weekday_or_weekend
    from int_taxi_tripdata_intrvl__feat_ext
)
select * from int_taxi_tripdata_intrvl

