
with
raw_rate_code_master AS ( select * from {{ref('raw_rate_code_master')}}),
stg_rate_code_master AS (
    select 
        "RatecodeID"    as rate_code_id,
        "RatecodeName"  as rate_code_name
    from raw_rate_code_master
)
select * from stg_rate_code_master