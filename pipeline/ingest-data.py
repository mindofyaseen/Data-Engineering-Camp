import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

@click.command()
@click.option('--user', default='root', help='PostgreSQL user')
@click.option('--password', default='root', help='PostgreSQL password')
@click.option('--host', default='pgdatabase', help='PostgreSQL host')
@click.option('--port', default=5432, type=int, help='PostgreSQL port')
@click.option('--db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--table', default='yellow_taxi_trips_2021_1', help='Target table name')
def ingest_data(user, password, host, port, db, table):
    # Extract year and month from table name (assuming format like yellow_taxi_trips_2021_1)
    parts = table.split('_')
    year = int(parts[-2])
    month = int(parts[-1])
    
    target_table = table
    chunksize = 100000
    
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    url_prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow'
    
    url = f'{url_prefix}/yellow_tripdata_{year:04d}-{month:02d}.csv.gz'
    
    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunksize
    )
    
    first_chunk = next(df_iter)
    
    first_chunk.head(0).to_sql(
        name=target_table,
        con=engine,
        if_exists="replace"
    )
    
    print(f"Table {target_table} created")
    
    first_chunk.to_sql(
        name=target_table,
        con=engine,
        if_exists="append"
    )
    
    print(f"Inserted first chunk: {len(first_chunk)} \n")
    
    for df_chunk in tqdm(df_iter):
        df_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists="append"
        )
        print(f"Inserted chunk: {len(df_chunk)} \n")
    
    print(f'done ingesting to {target_table} \n from {url}')

if __name__ == '__main__':
    ingest_data()