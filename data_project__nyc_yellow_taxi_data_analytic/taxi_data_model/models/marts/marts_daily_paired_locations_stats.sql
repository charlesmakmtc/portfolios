with
anly_paired_locations_stats_per_date AS (select * from {{ref('anly_paired_locations_stats_per_date')}}),
marts_daily_paired_locations_stats AS (
    select distinct
        trip_end_date
        ,pu_location_id 
        ,do_location_id
        ---- count by group
        ,cnt_paired_location_per_date
        --- count by side
        ,cnt_pu_location_id_per_date
        ,cnt_do_location_id_per_date
        --- total count per date
        ,cnt_per_date
    from anly_paired_locations_stats_per_date
)

select * 
from marts_daily_paired_locations_stats
order by trip_end_date, pu_location_id, do_location_id