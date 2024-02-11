import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@data_loader
def load_data_from_api(*args, **kwargs):
    url_list = [
        'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-01.parquet', 
        'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-02.parquet', 
        'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-03.parquet', 
        'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-04.parquet', 
        'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-05.parquet', 
        'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-06.parquet', 
        'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-07.parquet', 
        'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-08.parquet', 
        'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-09.parquet', 
        'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-10.parquet', 
        'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-11.parquet', 
        'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-12.parquet'
        ]
    
    taxi_dtypes = {
        'VendorID': 'Int64',
        'passenger_count': 'double',
        'trip_distance': 'double',
        'RatecodeID': 'double',
        'store_and_fwd_flag': 'string',
        'PULocationID': 'int64',
        'DOLocationID': 'int64',
        'payment_type': 'double',
        'fare_amount': 'double',
        'extra': 'double',
        'mta_tax': 'double',
        'tip_amount': 'double',
        'tolls_amount': 'double',
        'ehail_fee': 'null',
        'improvement_surcharge': 'double',
        'total_amount': 'double',
        'trip_type': 'double',
        'congestion_surcharge': 'double',
        'lpep_pickup_datetime': 'timestamp[us]',
        'lpep_dropoff_datetime': 'timestamp[us]'
    }


    # native date parsing t
    parse_dates = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']

    return pd.concat([
        pd.read_parquet(url, columns=taxi_dtypes.keys(), engine='pyarrow', use_pandas_metadata=True)
        for url in url_list
    ])


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

