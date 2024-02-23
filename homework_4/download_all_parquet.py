import requests

urls = [
    'https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2019-01.parquet',
    'https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2019-02.parquet',
    'https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2019-03.parquet',
    'https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2019-04.parquet',
    'https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2019-05.parquet',
    'https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2019-06.parquet',
    'https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2019-07.parquet',
    'https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2019-08.parquet',
    'https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2019-09.parquet',
    'https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2019-10.parquet',
    'https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2019-11.parquet',
    'https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2019-12.parquet'
]

for url in urls:
    filename = url.split('/')[-1]
    with open(filename, 'wb') as f:
        response = requests.get(url)
        f.write(response.content)
