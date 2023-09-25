import pandas as pd
import argparse
import os
from sqlalchemy import create_engine


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    database = params.database
    table_name = params.table_name
    url = params.url

    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'


    os.system(f'wget {url} -O {csv_name}')

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")
    engine.connect()


    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000, parse_dates=["tpep_pickup_datetime", "tpep_dropoff_datetime"])
    first_chunk = next(df_iter)
    first_chunk.to_sql(name=table_name, con=engine, if_exists='replace')
    for chunk in df_iter:
        chunk.to_sql(name=table_name, con=engine, if_exists='append')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres ')

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--database', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table where we will write the results to')
    parser.add_argument('--url', help='url of a csv file')

    args = parser.parse_args()

    main(args)