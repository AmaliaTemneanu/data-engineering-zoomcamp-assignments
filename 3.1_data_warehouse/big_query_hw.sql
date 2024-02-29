CREATE OR REPLACE EXTERNAL TABLE `crafty-ring-411313.ny_taxy.external_green_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = [
    'gs://yc-tl-data/trip data/green_tripdata_2022-*.parquet'
  ]
);

SELECT * FROM crafty-ring-411313.ny_taxy.external_green_tripdata limit 10;

SELECT COUNT(*) AS total_rows
FROM crafty-ring-411313.ny_taxy.external_green_tripdata;

CREATE OR REPLACE TABLE `crafty-ring-411313.ny_taxy.materialized_green_tripdata`
AS
SELECT *
FROM crafty-ring-411313.ny_taxy.external_green_tripdata;

SELECT COUNT(DISTINCT PULocationID) AS distinct_pu_location_ids
FROM `crafty-ring-411313.ny_taxy.external_green_tripdata`;

SELECT COUNT(DISTINCT PULocationID) AS distinct_pu_location_ids
FROM `crafty-ring-411313.ny_taxy.materialized_green_tripdata`;

SELECT COUNT(*) AS records_with_zero_fare
FROM `crafty-ring-411313.ny_taxy.external_green_tripdata`
WHERE fare_amount = 0;

CREATE OR REPLACE TABLE `crafty-ring-411313.ny_taxy.optimized_green_tripdata`
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PUlocationID
AS
SELECT *
FROM `crafty-ring-411313.ny_taxy.external_green_tripdata`;

SELECT DISTINCT PUlocationID
FROM `crafty-ring-411313.ny_taxy.materialized_green_tripdata`
WHERE lpep_pickup_datetime BETWEEN '2022-06-01' AND '2022-06-30';

SELECT DISTINCT PUlocationID
FROM `crafty-ring-411313.ny_taxy.optimized_green_tripdata`
WHERE lpep_pickup_datetime BETWEEN '2022-06-01' AND '2022-06-30';



