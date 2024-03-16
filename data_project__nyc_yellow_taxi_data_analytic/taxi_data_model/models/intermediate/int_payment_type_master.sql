
with
stg_payment_type_master AS ( select * from {{ref('stg_payment_type_master')}}),
int_payment_type_master AS (
    select 
        payment_type,
        payment_name
    from stg_payment_type_master
)
select * from int_payment_type_master