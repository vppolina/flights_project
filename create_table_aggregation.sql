-- create all tables

create table flights_all
(
id serial
,icao24 varchar(20)
,callsign varchar(20)
,origin_country varchar(50)
,time_position bigint
,last_contact bigint
,longitude float
,latitude float
,baro_altitude float
,on_ground boolean
,velocity float
,true_track float
,vertical_rate float
,sensors integer
,geo_altitude float
,squawk varchar(20)
,spi boolean
,position_source integer
,retrieved_at timestamp
)


create table airline_crashes
(
id serial
,operator varchar(5)
,place varchar(100)
,country varchar(100)
)


create table airline_codes
(
id serial
,icao varchar(5)
,airline varchar(100)
,callsign varchar(100)
,country varchar(100)
)


--data aggregation

with crashes as (
select 
	operator
	,count(distinct id) as nr_crashes
from airline_crashes
group by 1
order by 2 DESC
)
, codes as (
select 
	f.id
	, f.callsign
	, f.longitude
	, f.latitude
	, f.geo_altitude
	, f.origin_country
	, ac.icao
	, ac.airline
	, ac.country
from flights_all f
inner join airline_codes ac on substring(f.callsign, 1, 3) ilike ac.icao
)
select 
	id
	,callsign
	,longitude
	,latitude
	,geo_altitude
	,icao
	,origin_country
	,airline
	,country
	,nr_crashes
from codes c
left join crashes cr  on c.airline ilike cr.operator
