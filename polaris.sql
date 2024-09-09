SELECT
  'Credit Card' AS payment_type_1,
  CASE WHEN p2.payment_type = 1 THEN 'Credit card' WHEN p2.payment_type = 2 THEN 'Cash' WHEN p2.payment_type = 3 THEN 'No charge' WHEN p2.payment_type = 4 THEN 'Dispute' WHEN p2.payment_type = 5 THEN 'Unknown' WHEN p2.payment_type = 6 THEN 'Voided trip' END AS payment_type_2,
  p1.avg_tip_amount AS avg_tip_amount_1,
  p2.avg_tip_amount AS avg_tip_amount_2,
  ABS(p1.avg_tip_amount - p2.avg_tip_amount) AS difference,
  ((p1.avg_tip_amount - p2.avg_tip_amount) / p1.avg_tip_amount * 100) AS percent_change,
  CASE WHEN ((p1.avg_tip_amount - p2.avg_tip_amount) / p1.avg_tip_amount * 100) > 50 THEN 'Significantly Higher' WHEN p1.avg_tip_amount < p2.avg_tip_amount THEN 'Significantly Lower' ELSE 'No Significant Difference' END AS comparison_with_other_payment_types
FROM (
  SELECT
    payment_type,
    AVG(tip_amount) AS avg_tip_amount
  FROM "trips_xaa"
  WHERE payment_type IN (1, 2, 3, 4, 5, 6)
  GROUP BY payment_type
) AS p1
CROSS JOIN (
  SELECT
    payment_type,
    AVG(tip_amount) AS avg_tip_amount
  FROM "trips_xaa"
  WHERE payment_type IN (1, 2, 3, 4, 5, 6)
  GROUP BY payment_type
) AS p2
WHERE p1.payment_type = 1;

WITH earnings AS (
    SELECT 
        fare_amount + tip_amount AS total_earnings,
        ROW_NUMBER() OVER (ORDER BY fare_amount + tip_amount DESC) AS earnings_rank
    FROM 
        "trips_xaa"
     GROUP BY (fare_amount + tip_amount)
)
SELECT 
    COUNT(*)
FROM 
    earnings
WHERE 
    earnings_rank <= FLOOR(0.1 * (SELECT COUNT(*) FROM earnings));

SELECT RatecodeID,
CASE WHEN int_dy=1 THEN 'Mon' 
            WHEN int_dy=2 THEN 'Tue' 
            WHEN int_dy=3 THEN 'Wed'
            WHEN int_dy=4 THEN 'Thu'
            WHEN int_dy=5 THEN 'Fri'
            WHEN int_dy=6 THEN 'Sat'
            WHEN int_dy=7 THEN 'Sun' 
            END AS wk_dy,
"Hour",
avg_fare_per_mile
FROM (
SELECT 
    RatecodeID,
      TIME_EXTRACT(__time, 'DOW') AS "int_dy",
    TIME_EXTRACT(__time, 'HOUR') AS "Hour",
    SUM(fare_amount) / SUM(trip_distance) AS avg_fare_per_mile
FROM 
    "trips_xaa"
    where RatecodeID is not NULL
 GROUP BY 
    RatecodeID, TIME_EXTRACT(__time, 'DOW'), TIME_EXTRACT(__time, 'HOUR') ;
    )

SELECT
  "Year",
  "Month",
  CASE WHEN int_dy = 1 THEN 'Mon' WHEN int_dy = 2 THEN 'Tue' WHEN int_dy = 3 THEN 'Wed' WHEN int_dy = 4 THEN 'Thu' WHEN int_dy = 5 THEN 'Fri' WHEN int_dy = 6 THEN 'Sat' WHEN int_dy = 7 THEN 'Sun' END AS wk_dy,
  "Hour",
  total_trips
FROM (
  SELECT
    TIME_EXTRACT(__time, 'YEAR') AS "Year",
    TIME_EXTRACT(__time, 'MONTH') AS "Month",
    TIME_EXTRACT(__time, 'DOW') AS "int_dy",
    TIME_EXTRACT(__time, 'HOUR') AS "Hour",
    COUNT(*) AS total_trips
  FROM "trips_xaa"
  GROUP BY TIME_EXTRACT(__time, 'YEAR'), TIME_EXTRACT(__time, 'MONTH'), TIME_EXTRACT(__time, 'DOW'), TIME_EXTRACT(__time, 'HOUR')
  ORDER BY total_trips DESC
  LIMIT 5
);
