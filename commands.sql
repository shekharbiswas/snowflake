-- create worksheet
-- create data warehouse
-- create External stage 
-- load unstructured (JSON) / CSV files into stage from the AWS S3
-- create database and table

create or replace table trips
(tripduration integer,
starttime timestamp,
stoptime timestamp,
start_station_id integer,
start_station_name string,
start_station_latitude float,
start_station_longitude float,
end_station_id integer,
end_station_name string,
end_station_latitude float,
end_station_longitude float,
bikeid integer,
membership_type string,
usertype string,
birth_year integer,
gender integer);

-- Create an External Stage eg AWS
-- go to database -> public -> create stage
-- name : citibike_trips
-- source: s3://snowflake-workshop-lab/citibike-trips-csv/

-- Check how many staging files are there
-- check stage details
-- now you can list staging file without loading into the table trips
  
list @citibike_trips;

--create file format

create or replace file format csv type='csv'
  compression = 'auto' field_delimiter = ',' record_delimiter = '\n'
  skip_header = 0 field_optionally_enclosed_by = '\042' trim_space = false
  error_on_column_count_mismatch = false escape = 'none' escape_unenclosed_field = '\134'
  date_format = 'auto' timestamp_format = 'auto' null_if = ('') comment = 'file format for ingesting data for zero to snowflake';


--verify file format is created

show file formats in database citibike;


select count(*) from trips limit 10;
-- nothing will show

-- copy the data from stage to the table trips

copy into trips from @citibike_trips file_format=csv PATTERN = '.*csv.*' ;

--change warehouse size from small to large (4x)
alter warehouse test set warehouse_size='large';

show warehouses;

copy into trips from @citibike_trips
file_format=CSV;


select date_trunc('hour', starttime) as "date",
count(*) as "num trips",
avg(tripduration)/60 as "avg duration (mins)",
avg(haversine(start_station_latitude, start_station_longitude, end_station_latitude, end_station_longitude)) as "avg distance (km)"
from trips
group by 1 order by 1;


select
monthname(starttime) as "month",
count(*) as "num trips"
from trips
group by 1 order by 2 desc;

create table trips_dev clone trips;

create database weather;


use role sysadmin;

use warehouse new_wh;
WEATHER.PUBLIC.JSON_WEATHER_DATA
use database weather;

use schema public;



create table json_weather_data (v variant);

create stage nyc_weather
url = 's3://snowflake-workshop-lab/zero-weather-nyc';


list @nyc_weather;


copy into json_weather_data
from @nyc_weather 
    file_format = (type = json strip_outer_array = true);

select * from json_weather_data limit 10;



create or replace view json_weather_data_view as
select
    v:obsTime::timestamp as observation_time,
    v:station::string as station_id,
    v:name::string as city_name,
    v:country::string as country,
    v:latitude::float as city_lat,
    v:longitude::float as city_lon,
    v:weatherCondition::string as weather_conditions,
    v:coco::int as weather_conditions_code,
    v:temp::float as temp,
    v:prcp::float as rain,
    v:tsun::float as tsun,
    v:wdir::float as wind_dir,
    v:wspd::float as wind_speed,
    v:dwpt::float as dew_point,
    v:rhum::float as relative_humidity,
    v:pres::float as pressure
from
    json_weather_data
where
    station_id = '72502';


select * from json_weather_data_view
where date_trunc('month',observation_time) = '2018-01-01'
limit 20;



select weather_conditions as conditions
,count(*) as num_trips
from citibike.public.trips
left outer join json_weather_data_view
on date_trunc('hour', observation_time) = date_trunc('hour', starttime)
where conditions is not null
group by 1 order by 2 desc;


-- Time travel use case ( 24 hours to 90 days )


drop table json_weather_data;

--- error as we dropped the table
select * from json_weather_data limit 10;



-- restore the table
undrop table json_weather_data;

-- verify the table is back
select * from json_weather_data limit 10;


-- ROLL Back use case

-- Switch to correct schema

use role sysadmin;

use warehouse new_wh;

use database citibike;

use schema public;


update trips set start_station_name = 'oops';

-- will show that all the stations have same name oops


select
start_station_name as "station",
count(*) as "rides"
from trips
group by 1
order by 2 desc
limit 20;

-- this query shows the list of past queries with 'update' in the query text

select * from table(information_schema.query_history_by_session (result_limit=>100))
where query_text like 'update%' order by start_time desc limit 1;


-- all queries are saved in the information_schema

-- In Snowflake, we can simply run a command to find the query ID of the last UPDATE command and store it in a variable named $QUERY_ID.


set query_id =
(select query_id from table(information_schema.query_history_by_session (result_limit=>100))
where query_text like 'update%' order by start_time desc limit 1);

-- Use Time Travel to recreate the table with the correct station names:
-- this updates the table back to the state before running query_id / update query_id
create or replace table trips as
(select * from trips before (statement => $query_id));


-- previous state

select
start_station_name as "station",
count(*) as "rides"
from trips
group by 1
order by 2 desc
limit 20;


-- CREATE NEW ROLE and ADD USER

use role accountadmin;

create role junior_dba;

grant role junior_dba to user YOUR_USERNAME_GOES_HERE;

create role junior_dba;

-- you are assigning this role to yourself
grant role junior_dba to user sbiswas;


use role junior_dba;

-- On the top right you will see your new role JUNIOR_DBA

-- Give access of new_wh to the new role JUNIOR_DBA as it would not have access to it by default

use role accountadmin;

grant usage on warehouse new_wh to role junior_dba;


use role junior_dba;

use warehouse new_wh;

-- the new role has no access to the databases created - citibike and weather
-- grant access to the new role


use role accountadmin;

grant usage on database citibike to role junior_dba;

grant usage on database weather to role junior_dba;
