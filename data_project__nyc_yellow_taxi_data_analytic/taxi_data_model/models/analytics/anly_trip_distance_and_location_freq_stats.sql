

{% set stats_meas_vals = ['trip_distance', 'total_trip_time_in_minutes']%}
{% set stats_meas_funcs = ['min', 'max', 'stddev', 'avg'] %}


with 
dim_trip_distance_and_location AS (select * from {{ref('dim_trip_distance_and_location')}}),
fct_taxi_tripdata AS (select * from {{ref('fct_taxi_tripdata')}}),

anly_trip_distance_and_location_freq AS (
    select 
        --m.*,
        {{ dbt_utils.star(from=ref('dim_trip_distance_and_location'), relation_alias='m') }},
        --- total trip_distance per month
        f.trip_start_date,
        f.trip_start_year,
        f.trip_start_month,
        --- calculate averages
        case 
            when m.total_trip_time_in_minutes > 0
                then (m.trip_distance::numeric / m.total_trip_time_in_minutes::numeric)::numeric 
                else 0.0
        end as taximeter_per_minute
    from dim_trip_distance_and_location as m
    inner join fct_taxi_tripdata        as f on f.fct_row_index_id = m.fct_row_index_id
)

, anly_trip_distance_and_location_freq_stats AS (
    select 
        m.*
        ---- calculate stats measurement (min / max /stdiv / mean ) per month and date
        -- stats measurement per date
    {%- for stats_meas_val in stats_meas_vals %}
        {% for stats_meas_func in stats_meas_funcs -%}
            ,{{stats_meas_func}}(m.{{stats_meas_val}}) over (partition by trip_start_date) as {{stats_meas_func}}_{{stats_meas_val}}_per_date
        {% endfor -%}
    {% endfor %}
        -- stats measurement per month
    {%- for stats_meas_val in stats_meas_vals %}
        {% for stats_meas_func in stats_meas_funcs -%}
            ,{{stats_meas_func}}(m.{{stats_meas_val}}) over (partition by trip_start_month) as {{stats_meas_func}}_{{stats_meas_val}}_per_month
        {% endfor -%}
    {% endfor %}

    from anly_trip_distance_and_location_freq as m
)

select * from anly_trip_distance_and_location_freq_stats