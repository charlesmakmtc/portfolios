with
dim_trip_distance_and_location AS (select * from {{ref('dim_trip_distance_and_location')}}),


{% set locs_abbr = ['pu', 'do'] %}

{% set stats_aggr_funcs_lyr_1         = {'count': 'cnt'} %}
{% set stats_aggr_funcs_lyr_1_to_2    = {'min': 'min', 'max': 'max'} %}
{% set stats_aggr_column_name__cnt_per_date   = 'cnt_per_date' %}
{% set stats_aggr_column_name__cnt_per_yr_mth__DEV = 'cnt_per_yr_mth' %}

{% set aggr_column_name__categorical_columns   = ['1', '2'] %}


anly_paired_locations_freq_per_date__lyr_1_cnt AS (
    select distinct
        f.trip_end_date
        ,m.pu_location_id 
        ,m.do_location_id
        ,'--- count --->' as cnt_sep
        {% for aggr_func, aggr_abbr in stats_aggr_funcs_lyr_1.items() %}
            ,{{aggr_func}}(*) over (partition by f.trip_end_date, m.pu_location_id, m.do_location_id) as {{aggr_abbr}}_paired_location_per_date
            ,{{aggr_func}}(*) over (partition by f.trip_end_date) as {{stats_aggr_column_name__cnt_per_date}}
            {% for loc_abbr in locs_abbr -%}
                ,{{aggr_func}}(*) over (partition by f.trip_end_date, m.{{loc_abbr}}_location_id) as {{aggr_abbr}}_{{loc_abbr}}_location_id_per_date
            {% endfor %}
        {% endfor %}
    from dim_trip_distance_and_location as m
    inner join fct_taxi_tripdata        as f on f.fct_row_index_id = m.fct_row_index_id
)

,
anly_paired_locations_freq_per_date__lyr_1_aggr_cnt AS (
    select 
        m.*
        ,'--- aggregated count --->' as aggr_cnt_sep
        ---- aggreated count per date
        {% for loc_abbr in locs_abbr -%}
            {% for aggr_func, aggr_abbr in stats_aggr_funcs_lyr_1_to_2.items() %}
                ,{{aggr_func}}(m.cnt_{{loc_abbr}}_location_id_per_date) over (partition by m.{{stats_aggr_column_name__cnt_per_date}})::numeric as {{aggr_func}}_{{loc_abbr}}_location_id_per_date
            {% endfor -%}
        {% endfor -%}
    from anly_paired_locations_freq_per_date__lyr_1_cnt as m
)

,
anly_paired_locations_freq_per_date__lyr_1_aggr_cnt_ratio AS (
    select 
        m.* 
        ,'--- aggregated count ratio --->' as aggr_cnt_ratio
        {% for src_abbr in locs_abbr -%}
            {% for dst_abbr in locs_abbr -%}
                {% if src_abbr != dst_abbr %}
                ,(m.cnt_{{src_abbr}}_location_id_per_date::numeric / m.cnt_{{dst_abbr}}_location_id_per_date::numeric)::numeric as cnt_{{src_abbr}}_over_cnt_{{dst_abbr}}_ratio_per_date
            {% endif -%}
            {% endfor -%}
        {% endfor -%}
        
        {% for loc_abbr in locs_abbr -%}
            --- side-over-total ratio
            ,(m.cnt_{{loc_abbr}}_location_id_per_date::numeric / m.{{stats_aggr_column_name__cnt_per_date}}::numeric)::numeric as {{loc_abbr}}_occup_ratio_per_date
            --- aggregated side per date
            {% for aggr_func, aggr_abbr in stats_aggr_funcs_lyr_1_to_2.items() %}
                ,({{aggr_func}}(m.cnt_{{loc_abbr}}_location_id_per_date::numeric / m.{{stats_aggr_column_name__cnt_per_date}}::numeric) over (partition by m.{{stats_aggr_column_name__cnt_per_date}}))::numeric as {{aggr_func}}_{{loc_abbr}}_occup_ratio_per_date
            {% endfor -%}
        {% endfor -%}
    from anly_paired_locations_freq_per_date__lyr_1_aggr_cnt as m
)
,
anly_paired_locations_freq_per_date__lyr_1_norm_aggr_cnt_ratio AS (
    select 
        m.*
        ,'--- normalized aggregated count ratio --->' as norm_aggr_cnt_ratio_sep
        {% for loc_abbr in locs_abbr -%}
            ,
            case 
                when (max_{{loc_abbr}}_occup_ratio_per_date - min_{{loc_abbr}}_occup_ratio_per_date) > 0
                    then    (
                        ({{loc_abbr}}_occup_ratio_per_date - min_{{loc_abbr}}_occup_ratio_per_date) / (max_{{loc_abbr}}_occup_ratio_per_date - min_{{loc_abbr}}_occup_ratio_per_date)
                    ) 
                else -1.0 --- normalized value with negative value is abnormal
            end as norm_{{loc_abbr}}_occup_ratio_per_date
        {% endfor -%}
    from anly_paired_locations_freq_per_date__lyr_1_aggr_cnt_ratio as m
)
,
anly_paired_locations_freq_per_date__lyr_1_rnk_norm_aggr_cnt_ratio AS (
    select 
        m.*
        ,'--- ranking normalized aggregated count ratio --->' as rnk_norm_aggr_cnt_ratio_sep
        {% for loc_abbr in locs_abbr -%}
            ,dense_rank() over (partition by m.trip_end_date order by m.norm_{{loc_abbr}}_occup_ratio_per_date desc) as top_k_{{loc_abbr}}_occup_ratio_per_date
        {% endfor -%}
    from anly_paired_locations_freq_per_date__lyr_1_norm_aggr_cnt_ratio as m
)
,
anly_paired_locations_freq_per_date AS (
    select 
        m.* 
    from anly_paired_locations_freq_per_date__lyr_1_rnk_norm_aggr_cnt_ratio as m
)
select * 
from anly_paired_locations_freq_per_date
order by trip_end_date, pu_location_id, do_location_id
