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
,company
,taxi_id
,pickup_community_area
,dropoff_community_area
,trip_start_timestamp
,trip_start_date
,exact_trip_end_timestamp
,multi_day_flag
,case when date(h.date) is not null then 'Holidays' else 'Working Days' end as holiday_flag
,count(1) as total_trips
,sum(total_trip_seconds) sum_total_trip_seconds
,sum(trip_miles) sum_trip_miles
,sum(fare) sum_fare
,sum(tips) sum_tips
,sum(tolls) sum_tolls
,sum(extras) sum_extras
,sum(trip_total) sum_trip_total
,round(avg(total_trip_seconds),2) avg_total_trip_seconds
,round(avg(trip_miles),2) avg_trip_miles
,round(avg(fare),2) avg_fare
,round(avg(tips),2) avg_tips
,round(avg(tolls),2) avg_tolls
,round(avg(extras),2) avg_extras
,round(avg(trip_total),2) avg_trip_total
,round(SAFE_DIVIDE(sum(trip_total),sum(trip_miles)),2) as fare_per_mile
from 
{{ ref('stg_taxi_trips') }} t
left join {{ source('us_holiday_tdc', 'us_holiday') }} h
on date(h.date) = t.trip_start_date
and h.type like '%National holiday%'
{% if is_incremental() %}
WHERE trip_start_date > current_date - 2 -- or we can tax max of trip start date -2
{% endif %}
group by 1,2,3,4,5,6,7,8,9,10
