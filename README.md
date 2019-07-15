## Retrieving and Visualizing Real-time data from API 
HWR Berlin
Enterprise Architechtures for Big Data

Polina Voroshylova
Ana Maria Cuciuc
Tarazali Ryskul

# Project Scope

Analyse and visualize real-time flights data. Observe all possible flight open source data, retrive valuable and usuful information.
Try out new technologies, like Azure Cloud, Apache Airflow, Hadoop Spark.

# Database set up

We used Azure Cloud for 4 reasons:
1. We have a credit
2. It's so much easier to set up PostgreSQL database on this platform
3. Easy to maintain
4. Easy to connect

However, during the project we stack with several issues:
1. It's super slow - of course in the beginning we took 1 Core and 11 GB memory, nevertheless after switching to 2 Cores and 56 GB the speed remained the same, and this is the main reason why we reduced our data
2. Not possible to upload CSV files directly (Google Cloud does)
3. Not possible to set up PostgreSQL cron job directly usinf Basic subscription - they only provide super expensive (~ 500 EUR/month) cloud where cron jobs are supported

So this solution is nice for small projects with little data, but too expensive for production.

# Data sources 

* [**Open Sky Network**](https://opensky-network.org/apidoc/rest.html#all-state-vectors)

Open source data source with real-time flights data. You can retrieve data using either REST API, Python API or Java API. 
After some tryings, we decided to use REST API, it's easier to retrieve data and filter it right away.

* [**Airplane-Crashes-and-Fatalities**](https://opendata.socrata.com/Government/Airplane-Crashes-and-Fatalities-Since-1908/q2te-8cvq)

We decided to add additional information about crashes and include it to our project. Also we deep dived into this data and analysed it with Hadoop Spark.

* [**Airline Codes**](https://openflights.org/data.html)

Really nice data source. It contains lots of flights data. We used only airline codes data to join crashes data and align it with real-time flights data. However, there are no real-time data and flight roads data containes a lot of redundancies.

# Step 1 - Tables setup

After database creation we have to create 3 tables: flights_all, airline_crashes, airline_codes.
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
We alredy prepared .csv files, so we can just run small python code and insert the data into DB.

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

Airline crashes dataset was a bit tricky, it has many missing values and a city with a country concatinated in one column.
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

```
response = requests.get("https://opensky-network.org/api/states/all")
raw_data = json.loads(response.text)
data = raw_data['states']
```

