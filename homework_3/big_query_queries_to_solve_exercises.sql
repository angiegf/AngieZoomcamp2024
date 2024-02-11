SELECT * FROM `watchful-augury-411213.ny_taxi.green_taxi_2022`;

SELECT count(distinct PULocationID) as distinct_id_count FROM `watchful-augury-411213.ny_taxi.green_taxi_2022`;

SELECT count(1) as nr_rows FROM `watchful-augury-411213.ny_taxi.green_taxi_2022`;

SELECT count (1) as nr_records
FROM `watchful-augury-411213.ny_taxi.green_taxi_2022`
WHERE fare_amount = 0;

