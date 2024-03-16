
with 
    rate_code_master AS (
        select 1 as "RatecodeID", 'Standard rate' as "RatecodeName"
        UNION ALL
        select 2, 'JFK'
        UNION ALL
        select 3, 'Newark'
        UNION ALL
        select 4, 'Nassau or Westchester'
        UNION ALL
        select 5, 'Negotiated fare'
        UNION ALL
        select 6, 'Group ride'
    )
select * from rate_code_master