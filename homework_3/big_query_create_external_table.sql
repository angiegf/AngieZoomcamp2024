CREATE EXTERNAL TABLE `watchful-augury-411213.ny_taxi.green_taxi_2022`
  OPTIONS (
    format ="PARQUET",
    uris = ['gs://mage-zoomcamp-angie-gf-eu/green_taxi_2022.parquet']
    );