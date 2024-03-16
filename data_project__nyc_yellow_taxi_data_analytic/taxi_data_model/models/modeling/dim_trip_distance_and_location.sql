with
int_taxi_tripdata    AS (select * from {{ref('int_taxi_tripdata')}}),
int_rate_code_master AS (select * from {{ref('int_rate_code_master')}}),
int_taxi_tripdata_intrvl AS (select * from {{ref('int_taxi_tripdata_intrvl')}}),

dim_trip_distance_and_location__data_prep AS (
    select
        m.row_index_id as fct_row_index_id,
        --- business data (time & distance)
        m.pu_location_id,
        m.do_location_id,
        m.trip_distance,
        --- rate code
        case
            when r.rate_code_name is not null then r.rate_code_name
            else '<N/A>'
        end as rate_code_name,
        --- business (trip time) start / end time
        m_intrvl.total_trip_time,
        m_intrvl.total_trip_time_in_minutes
    from int_taxi_tripdata              as m
    left join int_rate_code_master      as r        on r.rate_code_id = m.rate_code_id
    inner join int_taxi_tripdata_intrvl as m_intrvl on m_intrvl.row_index_id = m.row_index_id
),
dim_trip_distance_and_location AS (
    select 
        m.*
    from dim_trip_distance_and_location__data_prep as m
)

select * from dim_trip_distance_and_location