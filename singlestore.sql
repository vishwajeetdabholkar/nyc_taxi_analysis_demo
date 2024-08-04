
-- This analysis identifies the top 10% of taxi rides by total earnings, considering both fare_amount and tip_amount.
WITH earnings AS (
    SELECT 
        fare_amount + tip_amount AS total_earnings,
        ROW_NUMBER() OVER (ORDER BY fare_amount + tip_amount DESC) AS earnings_rank,
        COUNT(*) OVER () AS total_count
    FROM yellow_tripdata_pl_new
)
SELECT *
FROM earnings
WHERE earnings_rank <= FLOOR(0.1 * total_count)
LIMIT 10;

-- This query compares average tip amounts across different payment types. It calculates the difference and percent change compared to credit card tips.
WITH credit_card_avg AS (
    SELECT AVG(tip_amount) AS cc_avg_tip
    FROM yellow_tripdata_pl_new
    WHERE payment_type = 1
)
SELECT
    CASE 
        WHEN payment_type = 1 THEN 'Credit card'
        WHEN payment_type = 2 THEN 'Cash'
        WHEN payment_type = 3 THEN 'No charge'
        WHEN payment_type = 4 THEN 'Dispute'
        WHEN payment_type = 5 THEN 'Unknown'
        WHEN payment_type = 6 THEN 'Voided trip'
    END AS payment_type,
    AVG(tip_amount) AS avg_tip,
    COUNT(*) AS trip_count,
    AVG(tip_amount) - cc_avg_tip AS difference,
    (AVG(tip_amount) - cc_avg_tip) / cc_avg_tip * 100 AS percent_change
FROM yellow_tripdata_pl_new, credit_card_avg
GROUP BY payment_type
ORDER BY avg_tip DESC;

-- This analysis breaks down the average fare per mile based on rate code, day of the week, and hour. The includes a CASE statement to display day names.
SELECT 
    RatecodeID,
    CASE 
        WHEN DAYOFWEEK(FROM_UNIXTIME(tpep_pickup_datetime / 1000000)) = 1 THEN 'Sun'
        WHEN DAYOFWEEK(FROM_UNIXTIME(tpep_pickup_datetime / 1000000)) = 2 THEN 'Mon'
        WHEN DAYOFWEEK(FROM_UNIXTIME(tpep_pickup_datetime / 1000000)) = 3 THEN 'Tue'
        WHEN DAYOFWEEK(FROM_UNIXTIME(tpep_pickup_datetime / 1000000)) = 4 THEN 'Wed'
        WHEN DAYOFWEEK(FROM_UNIXTIME(tpep_pickup_datetime / 1000000)) = 5 THEN 'Thu'
        WHEN DAYOFWEEK(FROM_UNIXTIME(tpep_pickup_datetime / 1000000)) = 6 THEN 'Fri'
        WHEN DAYOFWEEK(FROM_UNIXTIME(tpep_pickup_datetime / 1000000)) = 7 THEN 'Sat'
    END AS day_of_week,
    HOUR(FROM_UNIXTIME(tpep_pickup_datetime / 1000000)) AS hour_of_day,
    AVG(fare_amount / NULLIF(trip_distance, 0)) AS avg_fare_per_mile
FROM yellow_tripdata_pl_new
WHERE trip_distance > 0 AND RatecodeID IS NOT NULL
GROUP BY RatecodeID, day_of_week, hour_of_day
ORDER BY RatecodeID, day_of_week, hour_of_day
LIMIT 20;

-- This query identifies the top 5 busiest hours of the week. It now includes Year and Month, and uses a CASE statement for day names.
SELECT 
    YEAR(FROM_UNIXTIME(tpep_pickup_datetime / 1000000)) AS Year,
    MONTH(FROM_UNIXTIME(tpep_pickup_datetime / 1000000)) AS Month,
    CASE 
        WHEN DAYOFWEEK(FROM_UNIXTIME(tpep_pickup_datetime / 1000000)) = 1 THEN 'Sun'
        WHEN DAYOFWEEK(FROM_UNIXTIME(tpep_pickup_datetime / 1000000)) = 2 THEN 'Mon'
        WHEN DAYOFWEEK(FROM_UNIXTIME(tpep_pickup_datetime / 1000000)) = 3 THEN 'Tue'
        WHEN DAYOFWEEK(FROM_UNIXTIME(tpep_pickup_datetime / 1000000)) = 4 THEN 'Wed'
        WHEN DAYOFWEEK(FROM_UNIXTIME(tpep_pickup_datetime / 1000000)) = 5 THEN 'Thu'
        WHEN DAYOFWEEK(FROM_UNIXTIME(tpep_pickup_datetime / 1000000)) = 6 THEN 'Fri'
        WHEN DAYOFWEEK(FROM_UNIXTIME(tpep_pickup_datetime / 1000000)) = 7 THEN 'Sat'
    END AS day_of_week,
    HOUR(FROM_UNIXTIME(tpep_pickup_datetime / 1000000)) AS hour_of_day,
    COUNT(*) AS total_trips
FROM yellow_tripdata_pl_new
GROUP BY Year, Month, day_of_week, hour_of_day
ORDER BY total_trips DESC
LIMIT 5;

-- This query finds the shortest path between locations.
SELECT 
    PULocationID AS start_location,
    DOLocationID AS end_location,
    MIN(trip_distance) AS shortest_distance,
    AVG(trip_distance) AS avg_distance,
    COUNT(*) AS trip_count
FROM yellow_tripdata_pl_new
WHERE 
    PULocationID = 132 AND 
    DOLocationID = 10 AND 
    trip_distance > 0
GROUP BY PULocationID, DOLocationID
ORDER BY shortest_distance ASC
LIMIT 1;
