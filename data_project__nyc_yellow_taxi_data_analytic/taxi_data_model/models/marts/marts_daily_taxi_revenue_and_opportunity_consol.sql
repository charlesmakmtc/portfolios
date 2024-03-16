with
dim_trip_distance_and_location AS (select * from {{ref("dim_trip_distance_and_location")}}),
fct_taxi_tripdata AS (select * from {{ref("fct_taxi_tripdata")}}),
dim_trip_revenue AS ( select * from {{ref('dim_trip_revenue')}}),

anly_paired_locations_consol_per_date__data_prep AS (
    select
        f.fct_row_index_id
        -- trip start
        ,f.trip_start_date
        ,f.trip_start_weekday_name
        ,m.pu_location_id 
        -- trip end
        ,f.trip_end_date
        ,f.trip_end_weekday_name
        ,m.do_location_id
        -- revenue
        ,v.currency
        ,v.total_revenue_amount
    from dim_trip_distance_and_location as m
    inner join fct_taxi_tripdata        as f on f.fct_row_index_id = m.fct_row_index_id
    inner join dim_trip_revenue         as v on v.fct_row_index_id = f.fct_row_index_id
) 
,
anly_paired_locations_consol_per_date AS (
    select 
        trip_start_date,
        trip_start_weekday_name,
        pu_location_id,
        trip_end_date,
        trip_end_weekday_name,
        do_location_id,
        currency,
        sum(total_revenue_amount) as total_revenue_amount
    from anly_paired_locations_consol_per_date__data_prep
    group by
        trip_start_date,
        trip_start_weekday_name,
        pu_location_id,
        trip_end_date,
        trip_end_weekday_name,
        do_location_id,
        currency
)
select * from anly_paired_locations_consol_per_date