{{ config(
    materialized='view'
) }}

-- Assumptions 
-- For Multi day Trips, my assumption that drivers are taking rest from 10 PM to 6 AM
-- For Multi day Trips, my assumption that trip starting before 10 PM on start date rests at 10 PM
-- else trip duration will be counted till 12 midnight for that day
-- For Multi day Trips, my assumption that trip ending after 6 PM on end date rests till 6 AM
-- else trip duration will be counted till that time for that day
with main as 
(Select 
taxi_id
,trip_start_timestamp
,exact_trip_end_timestamp
,total_trip_seconds,
multi_day_flag 
from  {{ source('staging_taxi_trips_tdc', 'stg_taxi_trips') }} 
where  date(trip_start_timestamp) >='2023-01-01' 
)
Select distinct taxi_id,count(distinct date) as overworking_days from  
(Select taxi_id,date,sum(trip_duration_minutes) as work_duration
 from (Select 
taxi_id,date 
,max((case when multi_day_flag <>1 then total_trip_seconds 
        when multi_day_flag =1 and timestamp_flag = 'start' and extract( hour from trip_start_timestamp) >=22 then TIMESTAMP_DIFF(timestamp(date_add(date(trip_start_timestamp),Interval 1 day)), trip_start_timestamp, second)
        when multi_day_flag =1 and timestamp_flag = 'start' and extract( hour from trip_start_timestamp) <22 then TIMESTAMP_DIFF(timestamp_add(timestamp(date(trip_start_timestamp)),Interval 22 HOUR), trip_start_timestamp, second)
        when multi_day_flag = 1 and timestamp_flag = 'end' and extract( hour from exact_trip_end_timestamp) <6 then TIMESTAMP_DIFF(exact_trip_end_timestamp,timestamp(date(exact_trip_end_timestamp)), second)
        when multi_day_flag = 1 and timestamp_flag = 'end' and extract( hour from exact_trip_end_timestamp) >=6 then TIMESTAMP_DIFF(exact_trip_end_timestamp,timestamp_add(timestamp(date(exact_trip_end_timestamp)), INTERVAL 6 HOUR), second)
        end)/60) as trip_duration_minutes
from (
Select 'start' as timestamp_flag ,
taxi_id,
date(trip_start_timestamp) as date,
total_trip_seconds,
trip_start_timestamp,
exact_trip_end_timestamp ,
multi_day_flag
from main  
union all
Select 'end' as timestamp_flag ,
taxi_id,
coalesce(date(exact_trip_end_timestamp)) as date,
total_trip_seconds,
trip_start_timestamp,
exact_trip_end_timestamp ,
multi_day_flag
from main 
where DATE(exact_trip_end_timestamp ) <> DATE(trip_start_timestamp) 
) 
group by 1,2
)  group by 1,2 having work_duration >960 order by work_duration desc) 
 group by 1 order by overworking_days desc limit 100



