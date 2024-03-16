{% set locs_abbr = ['pu', 'do'] %}

with
anly_paired_locations_stats_per_date AS (select * from {{ref('anly_paired_locations_stats_per_date')}}),

marts_top_k_popular_pickup_dropoff_locations AS (
    --- [business opportunity] daily top 3 popluar pick-up locations
    {% for loc_abbr in locs_abbr -%}
    
    select distinct
        --- category
        trip_end_date,
        '{{loc_abbr}}'              as pickup_or_dropoff_abbr,
        {{loc_abbr}}_location_id    as location_id,
        --- counting
        cnt_{{loc_abbr}}_location_id_per_date as cnt_location_id_per_date,
        cnt_per_date,
        --- norm / ratio
        norm_{{loc_abbr}}_occup_ratio_per_date as norm_occup_ratio_per_date, 
        --- ranking
        top_k_{{loc_abbr}}_occup_ratio_per_date as top_k_occup_ratio_per_date
    FROM anly_paired_locations_stats_per_date
    where 
            --trip_end_date = '2020-01-31'
            top_k_{{loc_abbr}}_occup_ratio_per_date <= 3 -- top k = 3
        and norm_{{loc_abbr}}_occup_ratio_per_date >= 0 -- remove abnormal case
    
    {% if not loop.last -%} union all {% endif -%}
    {% endfor %}
)

select * 
from marts_top_k_popular_pickup_dropoff_locations
order by trip_end_date, pickup_or_dropoff_abbr desc, top_k_occup_ratio_per_date, location_id