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
,ifnull(cast(pickup_community_area as string),"Missing Pickup Area") as pickup_community_area
,ifnull(cast(dropoff_community_area as string),"Missing Drop Area") as dropoff_community_area
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
,ST_GEOGPOINT(pickup_longitude, pickup_latitude) as pickup_geopoint
,ST_GEOGPOINT(dropoff_longitude, dropoff_latitude) as dropoff_geopoint
,case when date(h.date) is not null then 'Holidays' else 'Working Days' end as holiday_flag
,round(SAFE_DIVIDE(trip_miles,trip_total),2) as fare_per_mile
 from 
{{ source('taxi_trips_tdc', 'taxi_trips') }} t
left join {{ source('us_holiday_tdc', 'us_holiday') }} h
on date(h.date) = date(t.trip_start_timestamp)
and h.type like '%National holiday%'
{% if is_incremental() %}
WHERE date(trip_start_timestamp) >= 'current_date - 2' -- or we can take max of trip start date -2
 {% endif %}
 -- Taking data from Year 2020 to 2023 as 2023 is the latest Year in the dataset