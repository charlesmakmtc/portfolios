with
dim_trip_distance_and_location AS (select * from {{ref('dim_trip_distance_and_location')}}),

anly_paired_locations_master AS (
    select distinct
         m.pu_location_id 
        ,m.do_location_id
    from dim_trip_distance_and_location as m
)

select * 
from anly_paired_locations_master
order by pu_location_id, do_location_id