

{% set tgt_table = 'dim_trip_distance_and_location' %}
{% set categorical_columns = ['pu_location_id', 'do_location_id'] %}
{% set value_columns = '<No column is required, count by grouped categorical_columns>' %}


{# function private variables #}

{% set stats_aggr_funcs_lyr_1         = {'count': 'cnt'} %}
{% set stats_aggr_funcs_lyr_1_to_2    = {'min': 'min', 'max': 'max'} %}
{% set stats_aggr_column_name__cnt_per_date   = 'cnt_per_date' %}

{#
input descriptions
-- aggr_funcs_lyr_1 = {'SQL aggregation function': 'aggregated abbreviation for column'}

/*
cte hierarchy (with intermediate)
1|-- tgt_table__lyr_1_cnt
2|-- tgt_table__lyr_1_aggr_cnt         (`aggr` included min/max)
3|-- tgt_table__lyr_1_aggr_cnt_ratio   
4|-- tgt_table__lyr_1_norm_aggr_cnt_ratio

5|-- output_table

,
tgt_table__lyr_1_aggr_cnt AS (...),
tgt_table__lyr_1_aggr_cnt_ratio AS (...),
tgt_table__lyr_1_norm_aggr_cnt_ratio AS (...),
final AS (...),
select * from final

*/
#}

with 
tgt_table__lyr_1_cnt AS (
    select
        {% for column_name in categorical_columns -%}
            {% if not loop.last -%} 
                {{column_name}},
            {% else -%}
                {{column_name}}
            {% endif -%}
        {% endfor %}
        ,'--- count --->' as cnt_sep
    from {{ ref(tgt_table) }}
) 
select * from tgt_table__lyr_1_cnt
