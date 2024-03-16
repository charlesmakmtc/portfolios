{% 
set get_table_columns = [
    {'dim_trip_distance_and_location': 'rate_code_name'},
    {'int_taxi_tripdata': 'pu_location_id'},
    {'int_taxi_tripdata': 'trip_start_date'}
    ] 
%}

{{ build_unique_column_values_corpus(get_table_columns=get_table_columns) }}
