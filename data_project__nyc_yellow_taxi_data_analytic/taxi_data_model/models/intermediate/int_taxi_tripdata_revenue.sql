with
int_taxi_tripdata AS ( select * from {{ref('int_taxi_tripdata')}}),

int_taxi_tripdata_revenue__feat_ext AS (
    select 
        row_index_id as fct_row_index_id,

        ---- currency
        'USD' as currency,
        --- revenue
        fare_amount, 
        extra, 
        mta_tax, 
        tip_amount, 
        tolls_amount, 
        improvement_surcharge, 
        total_amount, 
        congestion_surcharge, 
        airport_fee,
        ---- categorical amount grouping
        (total_amount + tip_amount) as total_revenue_amount
    from int_taxi_tripdata
)

,int_taxi_tripdata_revenue AS (
    select * 
    from int_taxi_tripdata_revenue__feat_ext
)

select * from int_taxi_tripdata_revenue