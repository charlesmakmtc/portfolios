with
int_taxi_tripdata AS ( select * from {{ref('int_taxi_tripdata')}}),
int_payment_type_master AS ( select * from {{ref('int_payment_type_master')}}),

dim_trip_payment AS (
    select
        m.row_index_id as fct_row_index_id,
        --- payment
        m.payment_type,
        pym.payment_name
    from int_taxi_tripdata as m
    inner join int_payment_type_master as pym on pym.payment_type = m.payment_type
)
select * from dim_trip_payment

