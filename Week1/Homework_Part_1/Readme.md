#Postgresql queries ran to answer questions 3 - 6

## Question 3

`SELECT
	count(*)
FROM
	green_taxi_data
WHERE
	DATE(lpep_pickup_datetime) = '2019-01-15' AND
	DATE(lpep_dropoff_datetime) = '2019-01-15';`
  
## Question 4

`SELECT
	lpep_pickup_datetime,
	trip_distance
FROM
	green_taxi_data
WHERE
	lpep_pickup_datetime BETWEEN '2019-01-10' AND '2019-01-28'
ORDER BY
	trip_distance DESC;`
  
## Question 5

`SELECT
	SUM(CASE WHEN passenger_count = 2 THEN 1 ELSE 0 END) AS "2_passengers",
	SUM(CASE WHEN passenger_count = 3 THEN 1 ELSE 0 END) AS "3_passengers"
FROM
	green_taxi_data
WHERE
	DATE(lpep_pickup_datetime) = '2019-01-01';`

## Question 6

`SELECT 
	CAST(lpep_pickup_datetime AS DATE) as "day",
	zpu."Zone" as "pickup_zone",
	zdo."Zone" as "dropoff_zone",
	tip_amount
FROM
	green_taxi_data t
JOIN
	zones zpu ON t."PULocationID" = zpu."LocationID"
JOIN
	zones zdo ON t."DOLocationID" = zdo."LocationID"
WHERE
	zpu."Zone" = 'Astoria'
ORDER BY
	tip_amount DESC;`
