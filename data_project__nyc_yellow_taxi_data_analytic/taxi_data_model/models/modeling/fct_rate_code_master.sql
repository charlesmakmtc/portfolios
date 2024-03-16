
with
int_rate_code_master AS ( select * from {{ref('int_rate_code_master')}}),
fct_rate_code_master AS (
    select 
        rate_code_id,
        rate_code_name
    from int_rate_code_master
)
select * from fct_rate_code_master