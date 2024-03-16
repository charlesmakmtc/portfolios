with
dim_calendar AS (select * from {{ref('dim_calendar')}}),
dim_trip_distance_and_location AS (select * from {{ref('dim_trip_distance_and_location')}}),
fct_taxi_tripdata    AS (select * from {{ref('fct_taxi_tripdata')}}),

{% set date_columns = ['trip_start_date', 'trip_end_date'] %}

anly_trip_distance_and_location__data_prep AS (
    select
        f.fct_row_index_id
        ---- intervals data
        {% for date_column in date_columns -%}
        ,min(f.{{date_column}}) over () as first_date_{{date_column}}
        ,max(f.{{date_column}}) over () as last_date_{{date_column}}
        {% endfor -%}
        --- 	trip time consumption
        ,m.total_trip_time
        ,m.total_trip_time_in_minutes
        ---- 	trip location (from/to)
        ,m.pu_location_id 
        ,m.do_location_id
        ----	trip distinace 
        ,m.trip_distance
        ,m.rate_code_name
        ---- exploration data
        {# ,{{ dbt_utils.star( ref('fct_taxi_tripdata'), relation_alias='f', except=['fct_row_index_id'])}}, #}
        {# ,{{ dbt_utils.star( ref('dim_trip_distance_and_location'), relation_alias='m', except=['fct_row_index_id'])}} #}
        
    from dim_trip_distance_and_location as m
    inner join fct_taxi_tripdata        as f on f.fct_row_index_id = m.fct_row_index_id
) --select * from anly_trip_distance_and_location__data_prep

select *
from anly_trip_distance_and_location__data_prep
