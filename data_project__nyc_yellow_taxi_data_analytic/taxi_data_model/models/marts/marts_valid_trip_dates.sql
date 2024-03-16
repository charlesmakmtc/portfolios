
{% set sql_get_first_valid_date %}
    select min(trip_end_date) from {{ref('fct_taxi_tripdata')}}
{% endset %}

{% set sql_get_last_valid_date %}
    select max(trip_end_date) from {{ref('fct_taxi_tripdata')}}
{% endset %}

{% set first_valid_date = dbt_utils.get_single_value(sql_get_first_valid_date) %}
{% set last_valid_date = dbt_utils.get_single_value(sql_get_last_valid_date) %}

with 
date_range AS (
{{ dbt_date.get_date_dimension(first_valid_date, last_valid_date) }}
),
mart_valid_trip_dates AS (
    select 
        '{{first_valid_date}}'::text::date as first_valid_date,
        '{{last_valid_date}}'::text::date as last_valid_date,
        date_day as valid_date 
    from date_range
)
select * from mart_valid_trip_dates

