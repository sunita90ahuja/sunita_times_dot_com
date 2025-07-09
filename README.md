Setup Instructions -
GCP setup
1- Create a Free GCP Account
2- Go to IAM & Admin Section
3- Create a Service Account with two roles - BigQuery Admin and BigQuery Job User
4- Create Json Key and download it on local machine

DBT setup 
1- Make sure Git is installed otherwise install it using "brew install git"
2- Install dbt big query - "pip install dbt-bigquery"
3- Initialize dbt project - "dbt init project_name"
4- After that follow sequence of steps for you to answer
5- It will create basic dbt folder structure inside project name folder and profiles.yml file inside 
.dbt folder
6- profiles.yml can be edited as per needs and json key path can be given here along with other attributes like dataset,project etc
7- Test the connection by using - "dbt debug"
8- DBT is connected with GCP big query

Github Setup
1- Create a account in Github
2- Then create a public project in Github 
3- Connect the local dbt folder structure to github repository - "git remote add origin https://github.com/sunita90ahuja/sunita_times_dot_com.git"
4- Then follow these commands - 
(i) git status
(ii) git add .
(iii) git commit -m "comment"
(iv)  git push origin main
5- Commits from local are added to Github repository

############################################################################################################

Project Structure
As latest data is till Dec,2023, staging is populated with data from 2020 Jan onwards.

1- Seeds folder - For loading csv dataset from Kaggle for US Holidays
2- Models folder - Having three folders - 
(i) Staging Folder - Cleaned data from public bigquery dataset as source
(ii) Summary Folder - Three summary views created based on company-payment,community areas and hours
(iii) SQL Query - For Overworking taxi ids
Schema.yml file in models folder for defining sources

############################################################################################################

Answers to Analytical Questions - 

1- Select taxi_id,sum(tips) as tips_total
   from staging_table
   where date(trip_start_timestamp) >='2023-10-01'
   group by 1 order by 2 desc limit 100;

*********************************************************************************

2- 
-- Assumptions 
-- For Multi day Trips, my assumption that drivers are taking rest from 10 PM to 6 AM
-- For Multi day Trips, my assumption that trip starting before 10 PM on start date rests at 10 PM
-- else work duration will be counted till 12 midnight for that day
-- For Multi day Trips, my assumption that trip ending after 6 PM on end date rests till 6 AM
-- else work duration will be counted till that time for that day (from 12 midnight)

with main as 
(Select 
taxi_id
,trip_start_timestamp
,exact_trip_end_timestamp
,total_trip_seconds,
multi_day_flag 
from  staging_table
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

*********************************************************************************

3- Taking into account only national holidays

    Select holiday_flag
        ,round(avg(num_trips),2) as avg_number_of_trips from 
        (
        Select  trip_start_date,
                case when date(h.date) is not null then 'Holidays' else 'Working Days' end as holiday_flag, count(1) as num_trips from staging_table  t
        left join holiday_table h
        on date(h.date) = date(t.trip_start_timestamp) and 
        type like '%National holiday%'
        where date(trip_start_timestamp) >='2023-01-01' 
        group by 1,2
        ) 
        group by 1 ;

*********************************************************************************
Other Insights added to Looker - 
   
4- Hourly Trend for Total fare and Fare Per Mile - 2023 
   Business Value - Can increase fares in peak hours

5- Top 10 Pickup Community Areas by AVG Trip Total -2023
   Business Value - Drivers can be placed in those zones,taxi stands,also can open marketing avenues

6- Total Trips by Payment Type with ZERO Fare but non ZERO trip duration - 2023
   Business Value - It can impact total revenue as fare is not counted properly

7- Drop Off Location based on latitude,longitude on map by Total Trip Fare -2023
   Business Value - Similar to Pickup location

8- Top 10 Company by Monthly Revenue - 2023
   Business value - Revenue is the base for any business,can do trend analysis across different months and forecast accordingly

Looker link - https://lookerstudio.google.com/u/0/reporting/1488efbd-0690-41db-a5f0-0d28f9ae2cb4/page/oDWQF

Converted UTC timing to Chicago zone


