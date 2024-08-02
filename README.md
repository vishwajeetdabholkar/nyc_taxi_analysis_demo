
# NYC Taxi Data Analysis Flask App

This repository contains a Flask application that provides data analysis capabilities for the NYC Taxi dataset. The application allows users to run predefined SQL queries, visualize the results, and gain insights into various aspects of taxi operations in New York City.

## Problem Statement

1. **Ranking and Percentiles**: Rank the top 10% of earnings based on the total earnings, considering both `fare_amount` and `tip_amount`.
2. **Payment Type Dynamics**: Determine if there is a significant difference in the average tip amount for credit card payments compared to other payment types.
3. **Dynamic Pricing Analysis**: Calculate the average fare amount per mile for each `RatecodeID`, taking into account the day of the week and time of day.
4. **Advanced Date and Time Analysis**: Identify the top 5 busiest hours of the week in terms of total trips.
5. **Geospatial Analysis**: Find the shortest path (in terms of distance) between two given locations (`PULocationID` and `DOLocationID`) using the available trip data.

## Features

- **Dropdown Menu**: Select from a set of predefined queries.
- **Query Execution**: Run the selected query against the NYC Taxi dataset stored in SingleStore.
- **Results Display**: Show the query results in both tabular form and graphical visualization.
- **Business and Technical Views**: Provide explanations for business users and technical details for data analysts.

## Setup and Installation

### Prerequisites

- Download the data from here : NYC Data Set [https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page]
- Save this parquet file data inside a s3 bucket, and use singlestore Schema and Pipeline Inference to create table and pipeline [https://docs.singlestore.com/cloud/load-data/load-data-with-pipelines/how-to-load-data-using-pipelines/schema-and-pipeline-inference/]

- Python 3.6 or higher
- Flask
- pandas
- plotly
- singlestoredb

### Installation

1. Clone the repository:
   ```sh
   git clone https://github.com/yourusername/nyc-taxi-data-analysis.git
   cd nyc-taxi-data-analysis
   ```

2. Create and activate a virtual environment:
   ```sh
   python3 -m venv venv
   source venv/bin/activate
   ```

3. Install the required packages:
   ```sh
   pip install -r requirements.txt
   ```

### Configuration

Setup the database using below like sytanx:
the * after NYC_taxi_data is a wildcard to select data from each Months folder.
```sql
CREATE INFERRED PIPELINE fhvhv_tripdata_pl AS LOAD DATA S3 's3://bucket/NYC_taxi_data/*/fhvhv_tripdata_2024-*.parquet' 
 CONFIG '{"region":"us-east-1"}' 
 CREDENTIALS '{"aws_access_key_id": "", 
  "aws_secret_access_key": "",
  "aws_session_token": ""
  }' 
  FORMAT PARQUET;
```
use below commands to check and verify the tables and pipeline created by INFERRE option:
```sql
show tables;
show pipeline;
show create table table_name;
show create pipeline pipeline_name;
```

Update the `DB_PARAMS` dictionary in `app.py` with your SingleStore database connection parameters.

### Running the Application

Start the Flask application:
```sh
flask run
```

Open your web browser and navigate to `http://127.0.0.1:5000` to access the application.

## Usage

1. Select a query from the dropdown menu.
2. Click "Run Analysis" to execute the query.
3. View the results in the "Analysis Results" section, which includes both the query and an explanation of the results.
4. If applicable, view the graphical representation of the results.

## Screenshots

### Home Page
<img width="1440" alt="image" src="https://github.com/user-attachments/assets/fec1f3b8-d6eb-4bbc-8b24-4b49ca9ffec3">

### Some sample results
<img width="936" alt="image" src="https://github.com/user-attachments/assets/e9f82f70-8dc7-43f0-b888-96417ecf59da">
<img width="936" alt="image" src="https://github.com/user-attachments/assets/3a5c957a-e486-46be-af90-3d17f876673b">


## Project Structure

```
nyc-taxi-data-analysis/
│
├── app.py               # Flask application
├── templates/
│   └── index.html       # HTML template for the app
├── static/
│   ├── css/
│   │   └── style.css    # Custom CSS styles
│   └── js/
│       └── script.js    # Custom JavaScript
├── requirements.txt     # List of Python dependencies
└── README.md            # This README file
```

## Queries Explained

### 1. Top 10% Earnings

**Query**:
```sql
WITH earnings_ranked AS (
    SELECT 
    VendorID, FROM_UNIXTIME(tpep_pickup_datetime / 1000000) as tpep_pickup_datetime, FROM_UNIXTIME(tpep_dropoff_datetime / 1000000) as tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, 
    store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, 
    improvement_surcharge, total_amount, congestion_surcharge, Airport_fee,
    PERCENT_RANK() OVER (ORDER BY (fare_amount + tip_amount) DESC) AS percentile
    from yellow_tripdata_pl_new
)
SELECT *
FROM earnings_ranked
WHERE percentile <= 0.1
LIMIT 50;
```

**Explanation**:
This query identifies the top 10% of taxi rides by total earnings, which includes both fare and tip amounts. Understanding these high-value trips can help optimize resource allocation and marketing strategies for premium services.

### 2. Payment Type Dynamics

**Query**:
```sql
SELECT 
    payment_type,
    AVG(tip_amount) AS avg_tip,
    AVG(tip_amount / fare_amount) AS avg_tip_percentage,
    COUNT(*) AS trip_count
FROM yellow_tripdata_pl_new
GROUP BY payment_type
ORDER BY avg_tip DESC;
```

**Explanation**:
By comparing average tip amounts and percentages across different payment methods, this query identifies trends that may inform decisions on payment system upgrades or driver incentive programs.

### 3. Dynamic Pricing Analysis

**Query**:
```sql
SELECT 
    RatecodeID,
    DAYOFWEEK(FROM_UNIXTIME(tpep_pickup_datetime / 1000000)) AS day_of_week,
    HOUR(FROM_UNIXTIME(tpep_pickup_datetime / 1000000)) AS hour_of_day,
    AVG(fare_amount / NULLIF(trip_distance, 0)) AS avg_fare_per_mile
FROM yellow_tripdata_pl_new
WHERE trip_distance > 0 and RatecodeID is not NULL
GROUP BY RatecodeID, day_of_week, hour_of_day
ORDER BY RatecodeID, day_of_week, hour_of_day;
```

**Explanation**:
This analysis breaks down the average fare per mile based on rate code, day of the week, and hour. It can help in developing dynamic pricing strategies and identifying the most profitable times and zones for drivers.

### 4. Busiest Hours

**Query**:
```sql
SELECT 
    DAYOFWEEK(FROM_UNIXTIME(tpep_pickup_datetime / 1000000)) AS day_of_week,
    HOUR(FROM_UNIXTIME(tpep_pickup_datetime / 1000000)) AS hour_of_day,
    COUNT(*) AS total_trips
FROM yellow_tripdata_pl_new
GROUP BY day_of_week, hour_of_day
ORDER BY total_trips DESC
LIMIT 5;
```

**Explanation**:
Identifying the top 5 busiest hours of the week allows for better resource allocation, surge pricing implementation, and driver shift planning to meet peak demand efficiently.

### 5. Shortest Path

**Query**:
```sql
SELECT 
    PULocationID, 
    DOLocationID, 
    MIN(trip_distance) AS shortest_distance,
    AVG(trip_distance) AS avg_distance,
    COUNT(*) AS trip_count
FROM yellow_tripdata_pl_new
GROUP BY PULocationID, DOLocationID
ORDER BY shortest_distance ASC
LIMIT 1;
```

**Explanation**:
Finding the shortest path between locations can optimize route planning, reduce operational costs, and improve customer satisfaction by minimizing travel time and distance.
