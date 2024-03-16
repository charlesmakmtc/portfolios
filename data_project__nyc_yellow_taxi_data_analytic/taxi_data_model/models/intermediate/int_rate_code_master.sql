
with
stg_rate_code_master AS ( select * from {{ref('stg_rate_code_master')}}),
int_rate_code_master AS (
    select 
        rate_code_id,
        rate_code_name
    from stg_rate_code_master
)
select * from int_rate_code_master