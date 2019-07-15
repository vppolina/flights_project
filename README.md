## Retrieving and Visualizing Real-time data from API 
HWR Berlin
Enterprise Architectures for Big Data

Polina Voroshylova
Ana Maria Cuciuc
Tarazali Ryskul

# Project Scope

Analyze and visualize real-time flights data. Observe all possible flight open source data, retrieve valuable and useful information.
Try out new technologies, like Azure Cloud, Apache Airflow, Hadoop Spark.

# Database set up

We used the Azure Cloud for 4 reasons:
1. We have a credit
2. It's so much easier to set up a PostgreSQL database on this platform
3. Easy to maintain
4. Easy to connect

However, during the project we stack with several issues:
1. It's super slow - of course, in the beginning, we took 1 Core and 11 GB memory, nevertheless, after switching to 2 Cores and 56 GB the speed remained the same, and this is the main reason why we reduced our data
2. Not possible to upload CSV files directly (Google Cloud does)
3. Not possible to set up PostgreSQL cron job directly using Basic subscription - they only provide super expensive (~ 500 EUR/month) cloud where cron jobs are supported

So this solution is nice for small projects with little data but too expensive for production.

# Data sources 

* [**Open Sky Network**](https://opensky-network.org/apidoc/rest.html#all-state-vectors)

Open source data source with real-time flights data. You can retrieve data using either REST API, Python API or Java API. 
After some trying, we decided to use REST API, it's easier to retrieve data and filter it right away.

* [**Airplane-Crashes-and-Fatalities**](https://opendata.socrata.com/Government/Airplane-Crashes-and-Fatalities-Since-1908/q2te-8cvq)

We decided to add additional information about crashes and include it to our project. Also, we deep dived into this data and analyzed it with Hadoop Spark.

* [**Airline Codes**](https://openflights.org/data.html)

Really nice data source. It contains lots of flights data. We used only airline codes data to join crashes data and align it with real-time flights data. However, there are no real-time data and flight roads data contains a lot of redundancies.

# Step 1 - Tables setup

After database creation, we have to create 3 tables: flights_all, airline_crashes, airline_codes.
We created simple SQL queries.


**Flights table**

```
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
```

**Airline crashes**

```
create table airline_crashes
(
id serial
,operator varchar(5)
,place varchar(100)
,country varchar(100)
)
```

*Airline codes*


```
create table airline_codes
(
id serial
,icao varchar(5)
,airline varchar(100)
,callsign varchar(100)
,country varchar(100)
)
```

# Step 2 - Data Retrieving

First, we have to upload "static" datasets on our DB: airline codes and airline crashes.
We already prepared .csv files, so we can just run a small python code and insert the data into DB.

**Airline Crashes**

```
#predefined functions to insert the data
def airline_codes(icao, airline, callsign, country):
    
    con = psycopg2.connect(database="postgres", user="polina@opensky", password="Bigdata2019", host="opensky.postgres.database.azure.com", port="5432")
    cursor = con.cursor()
    query = "INSERT INTO airline_codes (icao, airline, callsign, country) VALUES (%s, %s, %s, %s)"
    cursor.execute(query, (icao, airline, callsign, country))
    con.commit()
    
    cursor.close()
    con.close()
    return
    
#saving data to the Data Frame
data = pd.read_csv("airport_codes.csv", sep=';')

#iterating through data 
for index, row in data.iterrows():
    icao = row[0]
    airline = row[1]
    callsign = row[2]
    country = row[3]
    
    airline_codes(icao, airline, callsign, country)
```

**Airline Crashes**

Airline crashes dataset was a bit tricky, it has many missing values and a city with a country concatenated in one column.
That's why we aggregated data a bit and inserted into DB after this.

```
#predefined functions to insert the data
def airline_crashes(operator, place, country):
    
    con = psycopg2.connect(database="postgres", user="polina@opensky", password="Bigdata2019", host="opensky.postgres.database.azure.com", port="5432")
    cursor = con.cursor()
    query = "INSERT INTO airline_crashes (operator, place, country) VALUES (%s, %s, %s)"
    cursor.execute(query, (operator, place, country))
    con.commit()
    
    cursor.close()
    con.close()
    return
    
crashes = pd.read_csv("airline_crashes.csv", sep=';')

#splitting the Location column
new = crashes.Location.str.split(", ",expand=True)
crashes["Place"]= new[0] 
crashes["Country"]= new[1] 

#deleting existing Location column
crashes.drop(columns =["Location"], inplace = True) 

#replacing missing values
crashes.replace('NaN', None)

#iterating through Data Frame
for index, row in crashes.iterrows():
    operator = row[0]
    place = row[1]
    country = row[2]
    
    airline_crashes(operator, place, country)

```

**Retrieve data from API**

Second, we finally have to retrieve the data from the API. 
So we simply get the request from API and assign into the variable.
Since this .json file contains other data but flight states, we need to store only "states" data.

```
response = requests.get("https://opensky-network.org/api/states/all")
raw_data = json.loads(response.text)
data = raw_data['states']
```

Now we can just iterate through this data and store everything into DB.

```
def connect(icao24, callsign, origin_country, time_position, last_contact, longitude, latitude, baro_altitude, on_ground, velocity, true_track, vertical_rate, sensors, geo_altitude, squawk, spi, position_source):
    
    con = psycopg2.connect(database="postgres", user="polina@opensky", password="Bigdata2019", host="opensky.postgres.database.azure.com", port="5432")
    cursor = con.cursor()
    query = "INSERT INTO flights_all (icao24, callsign, origin_country, time_position, last_contact, longitude, latitude, baro_altitude, on_ground, velocity, true_track, vertical_rate, sensors, geo_altitude, squawk, spi, position_source, retrieved_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, current_timestamp)"
    cursor.execute(query, (icao24, callsign, origin_country, time_position, last_contact, longitude, latitude, baro_altitude, on_ground, velocity, true_track, vertical_rate, sensors, geo_altitude, squawk, spi, position_source))
    con.commit()
    
    cursor.close()
    con.close()
    return
    
    
def delete():
    con = psycopg2.connect(database="postgres", user="polina@opensky", password="Bigdata2019", host="opensky.postgres.database.azure.com", port="5432")
    cursor = con.cursor()
    query2 = "with flights as (select callsign from states where on_ground = true) delete from states where id in (select id from states s inner join flights f on f.callsign = s.callsign) or callsign ilike ''"
    cursor.execute(query2)
    con.commit()
    
    cursor.close()
    con.close()
    return
    
def insert(data):
    for d in range(len(data)):
        if (data[d][2] == 'Germany' or data[d][2] == 'Ukraine' or data[d][2] == 'Moldova' or data[d][2] == 'Kazakhstan'):
            icao24 = data[d][0]
            callsign = data[d][1]
            origin_country = data[d][2]
            time_position = data[d][3]
            last_contact = data[d][4]
            longitude = data[d][5]
            latitude = data[d][6]
            baro_altitude = data[d][7]
            on_ground = data[d][8]
            velocity = data[d][9]
            true_track = data[d][10]
            vertical_rate = data[d][11]
            sensors = data[d][12]
            geo_altitude = data[d][13]
            squawk = data[d][14]
            spi = data[d][15]
            position_source = data[d][16]
            print(origin_country)
        else: continue

        connect(icao24, callsign, origin_country, time_position, last_contact, longitude, latitude, baro_altitude, on_ground, velocity, true_track, vertical_rate, sensors, geo_altitude, squawk, spi, position_source)
```

As you might notice, we also created delete() function which filters the data and deletes all flights where callsign (flight number) is empty and there *on_ground = True* (which means that flight is either finished or haven't started yet). We will use it for **cron job**.

# Step 3 - Job Scheduling

Our goal was to visualize all flight in real time. We failed due to database low capacity, obviously. Nevertheless, we ran cron job for over 10 hours to get as much data as possible and demonstrate the whole concept.

*Why do we need cron jobs?*

This is a really nice tool to schedule some jobs, for example, to update DWH every 2 hours or to filter the data (as we did).

We tried two different tools: **Pentaho Data Integration** and **Apache Airflow**.

With the first one we already were familiar, another one is absolutely new, that's why we wanted to use Airflow instead of Pentaho.

Pentaho does it really smoothly, we only had to create transformation (but basically it's one Python script with all code above) and create the job with the scheduler in it. And it's done, running every 20 minutes.

The big disadvantage of Pentaho - is that it's not possible to run it on background on directly on the server. It's local solutions, which is also fine for some cases.

In the meantime, Apache Airflow may be connected to the server and run all jobs on the background no matter what. A really useful tool, especially when you have to run jobs every X hours/minutes and import data from the production database to DWH, and from DWH to another DWH.


# Step 4 - Data Visualization

For visualizations we used Tableau.
Since we have in total 3 different tables, we had to aggregate the data first, join them all and visualize in a nice way.

For data preparation we tried Tableau Prep, it has embedded functions, somehow similar to SQL, which is useful for aggregation. We needed to join flights table with airline codes data, but in flights table, we have only flight number and not the *icao* (airline code), so we used *LEFT* function in Tableau Prep to join these tables. 

Additionally, we wanted to add information about crashes, so we calculated a number of crashes per airline and join all it together.

Another way to aggregate data was **Custom SQL** in Tableau Desktop with is easier and faster. So we wrote the simple query and ran in Tableau.

```
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
```

As an output, we have one table which needs to be visualized. It saves a lot of time on aggregation and preparation part.

The whole visualization you can find [here]()
