
with 
    payment_type_master AS (
        select 1 as "payment_type", 'Credit card' as "payment_name"
        UNION ALL
        select 2, 'Cash'
        UNION ALL
        select 3, 'No charge'
        UNION ALL
        select 4, 'Dispute'
        UNION ALL
        select 5, 'Unknown'
        UNION ALL
        select 6, 'Voided trip'
    )
select * from payment_type_master