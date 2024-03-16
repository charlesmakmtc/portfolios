
with
int_payment_type_master AS ( select * from {{ref('int_payment_type_master')}}),
fct_payment_type_master AS (
    select 
        payment_type,
        payment_name
    from int_payment_type_master
)
select * from fct_payment_type_master