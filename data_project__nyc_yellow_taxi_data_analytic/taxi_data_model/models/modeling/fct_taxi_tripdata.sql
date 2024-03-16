with
int_taxi_tripdata AS ( select * from {{ref('int_taxi_tripdata')}}),
int_taxi_tripdata_intrvl AS ( select * from {{ref('int_taxi_tripdata_intrvl')}}),

fct_taxi_tripdata AS (
    select
        m.row_index_id as fct_row_index_id,
        m.row_created_at,
        m.row_updated_at,
        m.vendor_id,
        ---- business time data
        m.tpep_pickup_datetime                      as trip_start_datetime,
        m.tpep_dropoff_datetime                     as trip_end_datetime,
        ----  trip date features
        m.trip_start_year,
        m.trip_start_month,
        m.trip_start_date,
        m.trip_end_year,
        m.trip_end_month,
        m.trip_end_date,
        m_intrvl.trip_start_weekday_name, 
        m_intrvl.trip_end_weekday_name, 
        m_intrvl.trip_start_date_weekday_or_weekend, 
        m_intrvl.trip_end_date_weekday_or_weekend

    from int_taxi_tripdata as m
    inner join int_taxi_tripdata_intrvl as m_intrvl on m_intrvl.row_index_id = m.row_index_id
)
select * from fct_taxi_tripdata

