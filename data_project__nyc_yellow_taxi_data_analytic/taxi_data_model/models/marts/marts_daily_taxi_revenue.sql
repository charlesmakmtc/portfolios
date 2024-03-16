

with
fct_taxi_tripdata AS ( select * from {{ref('fct_taxi_tripdata')}}),
dim_trip_revenue AS ( select * from {{ref('dim_trip_revenue')}}),

marts_daily_taxi_revenue__data_prep AS (
    select 
        f.fct_row_index_id,
        f.trip_start_date,
        f.trip_end_date,
        d.currency,
        d.total_revenue_amount
    from fct_taxi_tripdata as f
    inner join dim_trip_revenue as d on d.fct_row_index_id = f.fct_row_index_id
),


marts_daily_taxi_revenue__lyr_1_aggr AS (
    select distinct
        m.trip_start_date,
        m.trip_end_date,
        m.currency,
        sum(m.total_revenue_amount) over (partition by m.trip_end_date, m.currency) as revenue_amount
    from marts_daily_taxi_revenue__data_prep as m
),

marts_daily_taxi_revenue__lyr_1_aggr_stats AS (
    select 
        *
        ,min(revenue_amount) over () as min_revenue_amount
        ,max(revenue_amount) over () as max_revenue_amount
    from marts_daily_taxi_revenue__lyr_1_aggr
),

marts_daily_taxi_revenue__lyr_1_norm_aggr AS (
    select 
        *
        ,'--- normalized sum --->' as norm_sep
        ,case 
            when (max_revenue_amount - min_revenue_amount) > 0
                then (revenue_amount - min_revenue_amount) / (max_revenue_amount - min_revenue_amount) 
            else -1.0
        end as norm_revenue_amount
    from marts_daily_taxi_revenue__lyr_1_aggr_stats
),

marts_daily_taxi_revenue AS (
    select * from marts_daily_taxi_revenue__lyr_1_norm_aggr
)
select * from marts_daily_taxi_revenue
order by revenue_amount desc, trip_end_date

