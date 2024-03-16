
with
raw_payment_type_master AS ( select * from {{ref('raw_payment_type_master')}}),
stg_payment_type_master AS (
    select 
        payment_type,
        payment_name
    from raw_payment_type_master
)
select * from stg_payment_type_master