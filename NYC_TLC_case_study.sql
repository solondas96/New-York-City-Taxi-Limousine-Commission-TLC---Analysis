-- Adding JAR file and Setting up the parameters..

ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-hcatalog-core-1.1.0-cdh5.11.2.jar;

SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;


-- Creating a database
create database if not exists NY_TLC;

-- Use this database to run further queries
use NY_TLC;

-- drop the table if it does not exist
drop table if exists Taxi_Data;

-- Creating external table. 

create external table if not exists Taxi_Data
(
VendorID int,
tpep_pickup_datetime timestamp,
tpep_dropoff_datetime timestamp,
passenger_count int,
trip_distance double,
RatecodeID int,
store_and_fwd_flag string,
PULocationID int,
DOLocationID int,
payment_type int,
fare_amount double,
extra double,
mta_tax double,
tip_amount double,
tolls_amount double,
improvement_surcharge double,
total_amount double
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/common_folder/nyc_taxi_data/'
tblproperties ("skip.header.line.count"="2"); 

-- Querying on the created table...
select * from Taxi_data limit 10;

-- Let's count the number of records in it...
select count(*) from Taxi_data;

-- Answer = 1174568

---------------------------------------------------------------------------------------------------
-- Data quality checks..


-- 1. How many records has each TPEP provider provided? Write a query that summarises the number of records of each provide?


select vendorid, count(*) from Taxi_data group by vendorid;


-- Answer: 
--	1. Vendor Id 1 : Creative Mobile Technologies, LLC =  527386 records
--	2. Vendor Id 2 : VeriFone Inc. = 647183 records

-- 2. The data provided is for months November and December only. Check whether the data is consistent, and if not, identify -- the data quality issues.


select count(*) 
from Taxi_data where tpep_pickup_datetime < '2017-11-1 00:00:00.0' or tpep_pickup_datetime >= '2018-01-01 00:00:00.0';

-- We see that 14 dates are out of range for tpep_pickup_datetime
-- The data spans across years ranging from 2003 to 2019.Majority of records lies in the year 2017 and for months of Nov and -- Dec and very few records belongs to other years


select count(*) 
from Taxi_data where tpep_dropoff_datetime < '2017-11-1 00:00:00.0' or tpep_dropoff_datetime >= '2018-01-01 00:00:00.0';


-- We see that 117 dates are out of range for tpep_dropoff_datetime
-- The data spans across years ranging from 2003 to 2019.Majority of records lies in the year 2017 and for months of Nov and -- Dec and very few records belongs to other years
-- Overall data looks pretty consistent for the months of Nov and Dec for year 2017 for both the columns (TPEP 
-- pickup_datetime and dropoff_datetime columns)

--Check which vendor has data out of the date range for TPEP pickup_datetime and dropoff_datetime columns

select  vendorid, count(*) from  Taxi_data 
where tpep_pickup_datetime < '2017-11-1 00:00:00.0' or tpep_pickup_datetime >= '2018-01-01 00:00:00.0'
group by vendorid;

-- Vendor 2 has 14 records which are out of range for tpep_pickup_datetime

select  vendorid, count(*) from  Taxi_data 
where tpep_dropoff_datetime < '2017-11-1 00:00:00.0' or tpep_dropoff_datetime >= '2018-01-01 00:00:00.0'
group by vendorid;

-- Vendor 1 has 29 records which has tpep_dropoff_datetime out of range
-- Vendor 2 has 88 records which has tpep_dropoff_datetime out of range


-- 3.Conclude which vendor is doing a bad job in providing the records using different columns of the dataset? 
-- Summarise your conclusions based on every column where these errors are present. 


-- a) VendorID: 
select distinct(VendorID) from Taxi_data;


-- We see that only 2 distincts vendors are present which is 1 and 2.
-- 1= Creative Mobile Technologies, LLC; 2= VeriFone Inc.


-- b) passenger_count
select passenger_count, count(*) from  Taxi_data  group by passenger_count;


-- We see that there are 6824 taxi rides that have 0 passenger count in it which is unusual.But we won't ignore the records -- with 0 passenger counts because it can be manual error.

select vendorid , count(passenger_count) from Taxi_data where passenger_count = 0 
group by vendorid;


-- Vendor 1 i.e. Creative Mobile Technologies, LLC is not providing proper passenger counts which are around 6813 odd records -- are having passenger count as 0.
-- However, Vendor 2 i.e. VeriFone Inc. has only 11 records with passenger count as 0.
-- So manual errors for passenger counts are made less by vendor 2

-- c) trip_distance: (Trip Distance cannot be negative)

select  vendorid,count(*) from  Taxi_data where trip_distance < 0 group by vendorid order by vendorid ;
-- We see that both vendor 1 and vendor 2 has no records with trip distance as negative which is a good sign

select  vendorid,count(*) from  Taxi_data where trip_distance = 0 group by vendorid order by vendorid ;

-- We see that both vendor 1 has 4217 records and vendor 2 has 3185 records with trip distance = 0.This seems unusual but we -- can ignore these records if we make an assumption that these taxis have not started taking any trips.Vendors might be at -- fault if they are taking rides and not recording thier respective distances as a result its showing as 0.



-- d) ratecodeid:
/*
These are the possible values of ratecodeid. 
1= Standard rate
2=JFK
3=Newark
4=Nassau or Westchester
5=Negotiated fare
6=Group ride
*/
-- Lets see which vendors have error records and how many.
select vendorid , count(*) from  Taxi_data where ratecodeid not in (1,2,3,4,5,6)
group by vendorid order by vendorid;


-- We see that 
-- vendor 1 has 8 records and 
-- vendor 2 has only 1 record as error records in ratecodeid
-- In total - there are 9 records for which the ratecode id is 99 which is invalid


-- e) store_and_fwd_flag
/*
Y= store and forward trip
N= not a store and forward trip
*/
select vendorid, count(*) from  Taxi_data
where store_and_fwd_flag not in ('Y','N')
group by vendorid order by vendorid; 
-- We see that there are no error records here for the column store_and_fwd_flag


-- f) Payment_type:
/*
These are the possible values of ratPayment_type.
1= Credit card
2= Cash
3= No charge
4= Dispute
5= Unknown
6= Voided trip
*/
select vendorid , count(*) from  Taxi_data
where Payment_type not in (1,2,3,4,5,6)
group by vendorid order by vendorid; 
-- We see that there are no error records here for each vendor


-- g) fare_amount:

select vendorid,count(fare_amount) from Taxi_data where fare_amount < 0
group by vendorid order by vendorid; 
-- We see that there are 558 records with negative fare amount and all are contributed by vendor 2

select vendorid,count(fare_amount) from Taxi_data
where fare_amount = 0
group by vendorid order by vendorid; 

-- We see that vendor 1 has 231 records and vendor 2 has 81 records with fare amount as 0. 
-- We can ignore these records because customers could have used some kind of coupons which may have allowed them to have --free rides.


-- h) extra: 
/* Miscellaneous extras and surcharges. Currently, this only includes the $0.50 and $1 rush hour and overnight charges */

select vendorid,count(*) from  Taxi_data where extra not in (0,0.5,1)
group by vendorid order by vendorid; 

-- We see that vendor 1 has 1823 records and vendor 2 has 3033 records with error data which does not include 0,0.5 or 1. 
-- vendor 2 is majorly at fault.


-- i) mta_tax:
/* $0.50 MTA tax that is automatically triggered based on the metered rate in use*/

select vendorid,count(*) from  Taxi_data where mta_tax not in (0,0.5)
group by vendorid order by vendorid; 

-- We see that vendor 1 has 1 record and vendor 2 has 547 records with error data which does not include 0 or 0.5. 
-- vendor 2 is majorly at fault.


-- j) Improvement_surcharge:
/* $0.30 improvement surcharge assessed trips at the flag drop*/

select vendorid,count(*) from  Taxi_data where Improvement_surcharge not in (0,0.3)
group by vendorid order by vendorid; 
-- We see that all the error records belong to vendor 2 which has a count of 562.

-- k) Tip_amount:
/* This field is automatically populated for credit card tips. Cash tips are not included */

select vendorid, count(*) from Taxi_data where Payment_type!=1 and tip_amount > 0
group by vendorid order by vendorid; 


-- 17 records have payment mode other than credit and still have tip amount greate than 0
-- Most of them are from vendor 1 where taxi drivers have taken tips which are not through credit cards


-- l) Tolls_amount: Total amount of all tolls paid in trip. 
select vendorid, count(*) from  Taxi_data where tolls_amount < 0
group by vendorid order by vendorid;

-- We see that all of the error negative toll amount is coming from vendor 2 which is 3 records


-- m) Total_amount: The total amount charged to passengers

select vendorid,count(*) from Taxi_data where  total_amount<0 
group by vendorid order by vendorid;

-- We see that vendor 2 has 558 records with negative total amount which is an error data.

---------------------------------------------------------------------------------------------------------------------------
-- Before going for analysis , we have to create a clean, ORC partitioned table for analysis and
-- Remove all the erroneous rows.

-- drop the table if it does not exist
drop table if exists Taxi_Data_partition;


-- We will be partitioning on the month column first as we need to answer question comparing between the two
-- since we expect only two month data to pass from out filters year is not an any use in partitioning.

-- Our second partition is based on Vendor, even if the question doesn't call for this one.

create external table if not exists Taxi_Data_partition
(
tpep_pickup_datetime timestamp,
tpep_dropoff_datetime timestamp,
passenger_count int,
trip_distance double,
RatecodeID int,
store_and_fwd_flag string,
PULocationID int,
DOLocationID int,
payment_type int,
fare_amount double,
extra double,
mta_tax double,
tip_amount double,
tolls_amount double,
improvement_surcharge double,
total_amount double
)partitioned by (Mnth int,VendorID int)
stored as orc location '/user/hive/warehouse/ny_tlc'
tblproperties ("orc.compress"="SNAPPY");

insert overwrite table Taxi_Data_partition partition( Mnth,VendorID )
select 
tpep_pickup_datetime,
tpep_dropoff_datetime,
passenger_count,
trip_distance,
RatecodeID,
store_and_fwd_flag,
PULocationID,
DOLocationID,
payment_type,
fare_amount,
extra,
mta_tax,
tip_amount,
tolls_amount,
improvement_surcharge,
total_amount,
month( tpep_pickup_datetime ) Mnth,
VendorID
from  Taxi_data
where  ( tpep_pickup_datetime >='2017-11-1 00:00:00.0' and tpep_pickup_datetime<'2018-01-01 00:00:00.0' ) and
( tpep_dropoff_datetime >= '2017-11-1 00:00:00.0' and tpep_dropoff_datetime<'2018-01-02 00:00:00.0' ) and
( tpep_dropoff_datetime > tpep_pickup_datetime) and
( passenger_count <> 0 ) and
( trip_distance >= 0 ) and 
( ratecodeid in (1,2,3,4,5,6) ) and
( fare_amount >= 0 ) and
( extra in (0,0.5,1) ) and
( mta_tax  in (0,0.5) ) and 
(( tip_amount >= 0 and Payment_type = 1 ) or ( Payment_type!=1 and tip_amount = 0 ) ) and
( tolls_amount >= 0 ) and
( improvement_surcharge in (0,0.3) ) and
( total_amount > 0 ) ;



-----------------------------------------------------------------------------------------------------------
-- Analysis 1:
-----------------------------------------------------------------------------------------------------------

-- 1. Compare the overall average fare per trip for November and December.

select mnth, round(avg(total_amount),2) as Avg_total_amt , round(avg(fare_amount),2) as Avg_fare_amount
from Taxi_Data_partition group by mnth;
-- Answerwer:
-- Month   Avg_total_amt   Avg_fare_amount
-- 12	      15.91	         12.71
-- 11	      16.21	         12.92

-- Insights
-- 1) Overall the month Novemeber seems to be better considering total amount.
-- 2) Also the difference in fare amount avg is on the lower side when compared to total amount
-- this signifies that extra tax and charges are also coming in play.

-- 2. Explore the ‘number of passengers per trip’.Do most people travel solo or with other people?

select passenger_count as Passengers,count(passenger_count) as Number_of_passengers 
from Taxi_Data_partition
group by passenger_count 
order by Number_of_passengers desc;

/*
Passengers	Number_of_passengers
1				819175
2				175128
5				54127
3				50270
6				32916
4				24734
7				4
8				2
*/

--Lets check the total number of records.
select count(*) as total_rec from Taxi_Data_partition;
-- Answer = 1156354

-- Lets see the Percentage count.
select passenger_count,round((count(*)*100/1156354),2) as percentage_count
from Taxi_Data_partition  
group by passenger_count
order by percentage_count desc;
/*
Answer: 
passenger_count      percentage_cnt
1	                70.9
2	                15.07
5               	4.7
3               	4.34
6               	2.85
4               	2.14
*/


-- Insights:
-- 1)Solo rides are most common , dominant infact with almost 71% of data belonging to them.
-- 2)Dual rides follow the trend of other significant category with 15% of the data.
-- 3)Rest all are below 5%.


-- 3. Which is the most preferred mode of payment?

select payment_type,round((count(*) * 100/1156354),2) percentage_count
from Taxi_Data_partition group by payment_type
order by percentage_count desc;

/*
Answer: 
--  payment_type     percentage_count
--  1                 	67.47 Credit card
--  2	                31.98 Cash
--  3	                0.43  No charge
--  4	                0.12  Dispute
*/


-- Insights:
-- 1) Credit card payments are dominant with 67.5% 
-- 2) Cash payment are 2nd highest paymnet 32%
-- 3) No charge (possibly free rides/passengers may have used coupons) are 3rd highest(but very less)
-- 4) Rest all modes are nearly negligible.


-- 4.What is the average tip paid per trip? Compare the average tip with the 25th, 50th and 75th percentiles 
-- and comment whether the ‘average tip’is a representative statistic (of the central tendency) of ‘tip amount paid’.

select round(avg(tip_amount),2) as Avg_tip_amount  from Taxi_Data_partition;

-- Answer:  Avg_tip_amount = 1.83


-- Lets check the percentiles 
select percentile_approx(tip_amount,array(0.25,0.50,0.75)) from Taxi_Data_partition;
/*
Answer:
25%   		0.0
50%		1.35 
75%		2.45
*/


-- Insights:
-- 1) 25th percentile records shows that they are being tipped zero.
-- 2) The median 1.35 is much lower then the avg 1.83 due to the skewness towards higher values.
-- 3) Hence mean is not representative statistic of centeral tendency here.4) It would be advised to use median instead of -- mean for this particular column during analysis.
-- 4) 75th percentile shows that people are paying tip amounts(2.45) to the vendors


-- 5. Explore the ‘Extra’ (charge) variable - what fraction of total trips have an extra charge is levied?

select extra, round(((count(*) * 100)/count(extra)),2) percentage_record from 
(select case when extra > 0 then 1 else 0 end  extra
from Taxi_Data_partition) T
group by extra
order by percentage_record desc;

/*
Answer:
   Extra      percentage_record
     0	            53.89
     1	            46.11
*/


-- Insights:
-- 1) The distribusion is evenly distributed with 46.11 % records having extra charges applied whereas 53.89 % have no extra charges applied.
-- 2) In other words,fraction of trips where extra are levied is 46%



----------------------------------------------------------------------------------------------------------------------
-- Analysis 2:
----------------------------------------------------------------------------------------------------------------------

-- 1. What is the correlation between the number of passengers on any given trip, and the tip paid per trip? Do multiple 
-- travellers tip more compared to solo travellers?


select round(corr(passenger_count, tip_amount),2) from Taxi_Data_partition;


-- Insights:
-- Correlation between passenger count and tip amount is very small but negative so it would be ok to say that passenger 
-- count is unrelated to the tip amount paid.


select solo, round(avg(tip_amount),2) as avg_tip from 
(select case when passenger_count = 1 then 1 else 0 end solo, tip_amount 
from Taxi_Data_partition) T group by solo;

/*
Answer: 
solo    avg_tip
 0	 1.80
 1       1.84
*/


-- Insights:
-- Both solo travellers and multiple travellers tip almost equally on an average.


-- 2. Segregate the data into five segments of ‘tip paid’: [0-5), [5-10), [10-15) , [15-20) and >=20. 
--    Calculate the percentage share of each bucket (i.e. the fraction of trips falling in each bucket).

select range_of_tip,count(*) as no_of_records, max(records_count)as Total_Records, 
round(count(*)*100/max(records_count),2) as Tip_Segment_fraction 
from (select T.*, count(*) over () records_count,
		case 	when (tip_amount >= 0 and tip_amount < 5)   then '[0-5)' 
				when (tip_amount >= 5 and tip_amount < 10)  then '[5-10)' 
				when (tip_amount >= 10 and tip_amount < 15) then '[10-15)'
				when (tip_amount >= 15 and tip_amount < 20) then '[15-20)'
				when (tip_amount >= 20) then '>=20' 
		end range_of_tip
		from Taxi_Data_partition T) as segments
group by range_of_tip
order by Tip_Segment_fraction desc;

/*
Answer:
range_of_tip	no_of_records	total_records	tip_segment_fraction
[0-5)		1068306		1156356			92.39
[5-10)		65161		1156356			5.64
[10-15)		19619		1156356			1.7
[15-20)		2189		1156356			0.19
>=20		1081		1156356			0.09
*/



-- Insights:
-- 1) 0-5 range is the most prominent group with 92.39% records which also falls in our 0 tip category and within 25 percentile.
-- 2) It says most people prefer not paying any tip.
-- 3) 5-10 has a 5.64 %, rest all the buckets are nearly negligible.


-- 3. Which month has a greater average ‘speed’ - November or December? 

select mnth , 
round(avg( trip_distance/((unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime))/3600 )),2) as avg_speed
from Taxi_Data_partition 
group by mnth 
order by avg_speed desc;

/*
Answer:
mnth    avg_speed
12	    11.05
11	    10.95
*/


-- Insights:
-- 1) Average speed of taxis in december is higher compared to Average speed in the month of November.
-- 2) December month is faster by 0.1 miles/hour.


-- 4. Analyse the average speed of the most happening days of the year, i.e. 31st December(New year’s eve) 
-- and 25th December(Christmas) and compare it with the overall average. 

--Query to check average speed for Christmas,New Year and Overall for December 2017
select 
round(avg(CASE when Mnth=12 and day(tpep_pickup_datetime)=25 THEN (trip_distance/((unix_timestamp(tpep_dropoff_datetime)-unix_timestamp(tpep_pickup_datetime))/3600)) ELSE null end),3) 
as Avg_Speed_Christmas_mph,
round(avg(CASE when Mnth=12 and day(tpep_pickup_datetime)=31 THEN (trip_distance/((unix_timestamp(tpep_dropoff_datetime)-unix_timestamp(tpep_pickup_datetime))/3600)) ELSE null end),3)
as Avg_speed_NewYearEve_mph,
round(avg(CASE when Mnth=12 THEN (trip_distance/((unix_timestamp(tpep_dropoff_datetime)-unix_timestamp(tpep_pickup_datetime))/3600))
else null end),3) as Avg_speed_Overall_mph
from Taxi_Data_partition;
/*
Answer:
avg_speed_christmas_mph		avg_speed_newyeareve_mph	avg_speed_overall_mph
	15.241				13.208				11.046
*/

-- Query to check speed for Overall(0) and Holiday(1)

select holiday, round(avg(speed),2) as avg_speed from 
(select case when ((tpep_pickup_datetime>='2017-12-25 00:00:00.0' and tpep_pickup_datetime<'2017-12-26 00:00:00.0') 
or (tpep_pickup_datetime>='2017-12-31 00:00:00.0' and tpep_pickup_datetime<'2018-01-01 00:00:00.0')  ) then 1 else 0 end holiday, 
trip_distance/((unix_timestamp(tpep_dropoff_datetime)-unix_timestamp(tpep_pickup_datetime) )/3600) speed
from Taxi_Data_partition) T
group by holiday
order by avg_speed desc;

/*
holiday      avg_speed
1	        13.98
0	        10.93
*/


-- Insights:
-- 1) Average speed on christmas is 15.241 mph
-- 2) Average speed on New year eve is 13.208 mph
-- 3) Overall average speed for the month nov and december is 13.98
-- 4) We see that average speed on a holiday is approximately 3 miles/hour more than on usual days which can be an indication -- of less traffic and clearer streets on holidays.
-- 5) Here we can conclude that average speed on both Christmas and New Year is higher than the overall average speed



-- Thank You
-------------------------------------------------------------------------------------------------------------------------------

