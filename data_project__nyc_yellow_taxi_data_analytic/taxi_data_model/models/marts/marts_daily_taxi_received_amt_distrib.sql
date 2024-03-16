

with
fct_taxi_tripdata AS (select * from {{ref('fct_taxi_tripdata')}}),
dim_trip_revenue AS ( select * from {{ref('dim_trip_revenue')}}),

unpivot_recv_amount_items AS (
    {{
    dbt_utils.unpivot(
        relation=ref("dim_trip_revenue"),
        cast_to="numeric",
        exclude=["fct_row_index_id", "currency", "airport_fee", "total_revenue_amount"],
        field_name="recv_item",
        value_name="recv_amt"
        )
    }}
)
,
marts_daily_taxi_recv_amt_distrib__data_prep_lyr_0 AS (
    select 
        f.fct_row_index_id,
        f.trip_end_date,
        -- unpivot received amount
        d.recv_item,
        d.recv_amt
    from fct_taxi_tripdata as f
    inner join unpivot_recv_amount_items as d on d.fct_row_index_id = f.fct_row_index_id
)
,
marts_daily_taxi_recv_amt_distrib__data_prep_lyr_1 AS (
    select distinct
        trip_end_date
        ,case 
            when recv_item in ('total_amount', 'tip_amount') then 'revenue category'
            else 'non-revenue category'
        end as recv_item_categy
        ,recv_item --recv_amt
        ,'--- sub-total received amount -->' as total_recv_amt_sep
        ,(sum(recv_amt) over (partition by recv_item, trip_end_date))::numeric   as total_recv_item_amt_per_date
        ,(sum(recv_amt) over (partition by trip_end_date))::numeric              as total_recv_amt_per_date
    from marts_daily_taxi_recv_amt_distrib__data_prep_lyr_0
) 

,
marts_daily_taxi_recv_amt_distrib__amt_ratio_distrib AS (
    select 
        *
        ,'--- received amount ratio distribution --->' as amt_ratio_distrib_sep
        ,case 
            when total_recv_amt_per_date > 0 
                then (total_recv_item_amt_per_date::numeric / total_recv_amt_per_date::numeric)::numeric 
            else 0.0
        end as item_over_total_amt_ratio_per_date

    from marts_daily_taxi_recv_amt_distrib__data_prep_lyr_1
) 
select * 
from marts_daily_taxi_recv_amt_distrib__amt_ratio_distrib 
order by trip_end_date, recv_item
