import pandas as pd
from time import time
from sqlalchemy import create_engine
import argparse
import os

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    #download the csv
    csv_name = 'output.csv'
    os.system(f'wget {url} -O {csv_name}')

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df = pd.read_csv(csv_name, compression='gzip')

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000, compression='gzip')
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    while True:
        t_start = time()
        df = next(df_iter)
        
        df['passenger_count'] = df['passenger_count'].astype('Int64')
        df.to_sql(name='green_taxi_data', con=engine, if_exists='append')
        
        t_end = time()
        
        print(f'insert chunk in {t_end - t_start} seconds')  

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Ingest CSV data to postgres')

    parser.add_argument('--user', help='username for database')
    parser.add_argument('--password', help='password for database')
    parser.add_argument('--host', help='host for database')
    parser.add_argument('--port', help='port for database')
    parser.add_argument('--db', help='database for database')
    parser.add_argument('--table_name', help='table name for database')
    parser.add_argument('--url', help='csv url for database')

    args = parser.parse_args()
    main(args)
