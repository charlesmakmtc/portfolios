with
anly_paired_locations_master AS (select * from {{ref('anly_paired_locations_master')}}),

marts_paired_locations_master AS  (
    select * 
    from anly_paired_locations_master
)
select * from marts_paired_locations_master