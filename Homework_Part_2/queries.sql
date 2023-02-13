SELECT
  count(*)
FROM
  `dezoomcamp.external_yellow_tripdata`;
  
SELECT
  count(*)
FROM
  `dezoomcamp.external_yellow_tripdata`
WHERE
  double_field_3 IS NULL and double_field_4 IS NULL;
  
SELECT 
  COUNT(DISTINCT(string_field_0)) 
FROM 
  `weighty-smoke-376109.dezoomcamp.yellow_tripdata`;
