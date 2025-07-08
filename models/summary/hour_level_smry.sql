{{ config(
    materialized='view'
) }}

Select
extract(year from trip_start_date) as trip_start_date_year
,extract(hour from trip_start_timestamp) as trip_start_hour
,count(1) as total_trips
,round(sum(total_trip_seconds/60),2) sum_total_trip_minutes
,round(sum(trip_miles),2) sum_trip_miles
,round(sum(fare),2) sum_fare
,round(sum(tips),2) sum_tips
,round(sum(tolls),2) sum_tolls
,round(sum(extras),2) sum_extras
,round(sum(trip_total),2) sum_trip_total
,round(avg(total_trip_seconds/60),2) avg_total_trip_minutes
,round(avg(trip_miles),2) avg_trip_miles
,round(avg(fare),2) avg_fare
,round(avg(tips),2) avg_tips
,round(avg(tolls),2) avg_tolls
,round(avg(extras),2) avg_extras
,round(avg(trip_total),2) avg_trip_total
,round(SAFE_DIVIDE(sum(trip_miles),sum(trip_total)),2) as fare_per_mile
from 
{{ source('staging_taxi_trips_tdc', 'stg_taxi_trips') }} 
group by 1,2 order by 1 desc,3 desc
