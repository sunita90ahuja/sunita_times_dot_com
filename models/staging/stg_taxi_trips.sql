{{ config(
    materialized='incremental',
    unique_key='unique_key',
    incremental_strategy='merge',
    partition_by={
      "field": "trip_start_date",
      "data_type": "date"
    }

) }}

Select 
unique_key
,taxi_id
,trip_start_timestamp
,date(trip_start_timestamp) as trip_start_date
,coalesce(TIMESTAMP_ADD(trip_start_timestamp, INTERVAL (trip_seconds) SECOND),(trip_end_timestamp))as exact_trip_end_timestamp
,coalesce(trip_seconds,TIMESTAMP_DIFF(trip_end_timestamp, trip_start_timestamp, SECOND)) as total_trip_seconds
,ifnull(trip_miles,0.0) as trip_miles
,pickup_census_tract
,dropoff_census_tract
,pickup_community_area
,dropoff_community_area
,ifnull(fare,0.0) as fare
,ifnull(tips,0.0) as tips
,ifnull(tolls,0.0) as tolls
,ifnull(extras,0.0) as extras
,ifnull(trip_total,0.0) as trip_total
,payment_type
,company
,pickup_latitude
,pickup_longitude
,pickup_location
,dropoff_latitude
,dropoff_longitude
,dropoff_location
,case when coalesce(date(TIMESTAMP_ADD(trip_start_timestamp, INTERVAL (trip_seconds) SECOND)),date(trip_end_timestamp))>date(trip_start_timestamp) then 1 else 0 end as multi_day_flag
 from 
{{ source('taxi_trips_tdc', 'taxi_trips') }}
{% if is_incremental() %}
WHERE date(trip_start_timestamp) > current_date - 2 -- or we can take max of trip start date -2
 {% endif %}
