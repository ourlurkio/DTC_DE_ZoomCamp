import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

@task(log_prints=True)
def fetch_data(dataset_url: str) -> pd.DataFrame:
    """Fetch data from url and return pd.Dataframe"""

    df = pd.read_csv(dataset_url, compression="gzip")
    return df

@task(log_prints=True)
def write_df_locally(df: pd.DataFrame) -> None:
    """Write df as a csv file, and if file already exists, append data"""

    file_path = f"data/holding_file.csv.gz"

    try:
        with open(file_path, 'a') as f:
            df.to_csv(f, index=False, compression='gzip', header=False)
    except FileNotFoundError:
        df.to_csv(file_path, index=False, compression='gzip')

@task(log_prints=True)
def write_to_gcs(input_path: str, gcs_path:str) -> None:
    """Write data from local path to GCS bucket"""

    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=input_path, to_path=gcs_path)
    return

@flow()
def etl_web_to_gcs() -> None:
    """Main ETL flow"""

    year = 2019
    months = [f'{month:02d}' for month in range(1, 13)]
    input_path = "data/holding_file.csv.gz"
    gcs_path = f"data/fhv/fhv_data_{year}.csv.gz"

    for month in months:
        dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_{year}-{month}.csv.gz"
        print(dataset_url)
        df = fetch_data(dataset_url)
        write_df_locally(df)
    
    write_to_gcs(input_path, gcs_path)

if __name__ == "__main__":
    etl_web_to_gcs()

    
