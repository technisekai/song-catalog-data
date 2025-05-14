from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from datetime import datetime
from cores.auth import spotify_auth, youtube_auth, clickhouse_conn
from cores.extract import get_metadata_from_spotify, get_metadata_from_youtube, get_data_from_gsheet
from cores.ingest import ingestion_into_clickhouse
import requests

# DAG Configuration
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False
}

def collect_songs_catalog(url: str, api_key: str, sheet_name: str, start_cell: str, end_cell: str):
    # Get songs catalog from gsheet
    results = get_data_from_gsheet(
        url=url,
        api_key=api_key,
        sheet_name=sheet_name,
        start_cell=start_cell,
        end_cell=end_cell
    )
    return results

def collect_metadata_from_spotify(keyword: str, spotify_client_id: str, spotify_client_secret: str, limit: int = 2):
    # Authentication
    auth = spotify_auth(
        auth_url="https://accounts.spotify.com/api/token", 
        spotify_client_id=spotify_client_id,
        spotify_client_secret=spotify_client_secret
    )
    # Get metadata
    results = get_metadata_from_spotify(
        search_track_url="https://api.spotify.com/v1/search",
        keyword=keyword,
        access_token=auth["access_token"],
        limit_results=limit
    )
    return results

def collect_metadata_from_youtube(keyword: str, api_key: str, limit: int = 2):
    # Authentication
    auth = youtube_auth(
        api_key=api_key
    )
    # Get metadata
    results = get_metadata_from_youtube(
        keyword=keyword,
        auth=auth,
        limit_results=limit
    )
    return results

def ingest_songs_catalog_bronze(
        google_api_key: str,
        gsheet_url: str,
        gsheet_sheet_name: str,
        gsheet_start_cell: str,
        gsheet_end_cell: str, 
        dwh_host: str,
        dwh_port: str,
        dwh_username: str,
        dwh_password: str,
        dwh_database: str
):
    results = collect_songs_catalog(
        url=gsheet_url,
        sheet_name=gsheet_sheet_name,
        start_cell=gsheet_start_cell,
        end_cell=gsheet_end_cell,
        api_key=google_api_key
    )
    ch_conn = clickhouse_conn(
        host=dwh_host,
        port=dwh_port,
        username=dwh_username,
        password=dwh_password,
        database=dwh_database
    )
    ingestion_into_clickhouse(
        conn=ch_conn,
        table_name="gsheet_songs_catalog",
        data=results
    )
    
def ingest_spotify_metadata_bronze(
        google_api_key: str,
        gsheet_url: str,
        gsheet_sheet_name: str,
        gsheet_start_cell: str,
        gsheet_end_cell: str, 
        spotify_client_id: str,
        spotify_client_secret: str,
        dwh_host: str,
        dwh_port: str,
        dwh_username: str,
        dwh_password: str,
        dwh_database: str,
        limit_song_search: int = 2
):
    gsheet_catalog_songs = f"{gsheet_url}/values/{gsheet_sheet_name}!{gsheet_start_cell}:{gsheet_end_cell}?key={google_api_key}"
    response = requests.get(gsheet_catalog_songs)
    if response.status_code == 200:
        rows = response.json().get("values", [])
        for row in rows:
            results = collect_metadata_from_spotify(
                keyword=row[2],
                spotify_client_id=spotify_client_id,
                spotify_client_secret=spotify_client_secret,
                limit=limit_song_search
            )
            results = [{**x, "code": row[0]} for x in results]
            ch_conn = clickhouse_conn(
                host=dwh_host,
                port=dwh_port,
                username=dwh_username,
                password=dwh_password,
                database=dwh_database
            )
            ingestion_into_clickhouse(
                conn=ch_conn,
                table_name="spotify_metadata",
                data=results
            )

def ingest_youtube_metadata_bronze(
        google_api_key: str, 
        gsheet_url: str,
        gsheet_sheet_name: str,
        gsheet_start_cell: str,
        gsheet_end_cell: str, 
        dwh_host: str,
        dwh_port: str,
        dwh_username: str,
        dwh_password: str,
        dwh_database: str, 
        limit_song_search: int = 2
):
    gsheet_catalog_songs = f"{gsheet_url}/values/{gsheet_sheet_name}!{gsheet_start_cell}:{gsheet_end_cell}?key={google_api_key}"
    response = requests.get(gsheet_catalog_songs)
    if response.status_code == 200:
        rows = response.json().get("values", [])
        for row in rows:
            results = collect_metadata_from_youtube(
                keyword=row[2],
                api_key=google_api_key,
                limit=limit_song_search
            )
            results = [{**x, "code": row[0]} for x in results]
            ch_conn = clickhouse_conn(
                host=dwh_host,
                port=dwh_port,
                username=dwh_username,
                password=dwh_password,
                database=dwh_database
            )
            ingestion_into_clickhouse(
                conn=ch_conn,
                table_name="youtube_metadata",
                data=results
            )


with DAG(
    "songs_catalaog_bronze",
    default_args=default_args,
    description="",
    schedule_interval=None,
    start_date=datetime(2025, 4, 4),
    catchup=False,
) as dag:
    # Declare variables
    api_key = Variable.get(key="api-key-secret", deserialize_json=True)
    bronze_config = Variable.get(key="bronze-config", deserialize_json=True)
    dwh_creds = Variable.get(key="dwh-creds-secret", deserialize_json=True)
    cnt_songs_catalog_scrap = int(bronze_config["gsheet_end_cell"][1])

    start = DummyOperator(task_id="start", dag=dag)

    end = DummyOperator(task_id="end", dag=dag)

    songs_catalog_ingestion = PythonOperator(
        task_id=f"ingest_songs_catalog_into_bronze",
        python_callable=ingest_songs_catalog_bronze,
        op_kwargs={
            "google_api_key": api_key["google_api_key"],
            "gsheet_url": bronze_config["gsheet_songs_catalog"],
            "gsheet_sheet_name": bronze_config["gsheet_sheet_name"],
            "gsheet_start_cell": bronze_config["gsheet_start_cell"],
            "gsheet_end_cell": bronze_config["gsheet_end_cell"], 
            "dwh_host": dwh_creds["dwh_host"],
            "dwh_port": dwh_creds["dwh_port"],
            "dwh_username": dwh_creds["dwh_username"],
            "dwh_password": dwh_creds["dwh_password"],
            "dwh_database": "bronze"
        }
    )

    spotify_metadata_ingestion = PythonOperator(
        task_id=f"ingest_spotify_metadata_into_bronze",
        python_callable=ingest_spotify_metadata_bronze,
        op_kwargs={
            "google_api_key": api_key["google_api_key"],
            "gsheet_url": bronze_config["gsheet_songs_catalog"],
            "gsheet_sheet_name": bronze_config["gsheet_sheet_name"],
            "gsheet_start_cell": "A2",
            "gsheet_end_cell": bronze_config["gsheet_end_cell"], 
            "spotify_client_id": api_key["spotify_client_id"],
            "spotify_client_secret": api_key["spotify_client_secret"],
            "dwh_host": dwh_creds["dwh_host"],
            "dwh_port": dwh_creds["dwh_port"],
            "dwh_username": dwh_creds["dwh_username"],
            "dwh_password": dwh_creds["dwh_password"],
            "dwh_database": "bronze"
        }
    )

    youtube_metadata_ingestion = PythonOperator(
        task_id=f"ingest_youtube_metadata_into_bronze",
        python_callable=ingest_youtube_metadata_bronze,
        op_kwargs={
            "google_api_key": api_key["google_api_key"],
            "gsheet_url": bronze_config["gsheet_songs_catalog"],
            "gsheet_sheet_name": bronze_config["gsheet_sheet_name"],
            "gsheet_start_cell": "A2",
            "gsheet_end_cell": bronze_config["gsheet_end_cell"], 
            "dwh_host": dwh_creds["dwh_host"],
            "dwh_port": dwh_creds["dwh_port"],
            "dwh_username": dwh_creds["dwh_username"],
            "dwh_password": dwh_creds["dwh_password"],
            "dwh_database": "bronze"
        }
    )

    start >> [songs_catalog_ingestion, youtube_metadata_ingestion, spotify_metadata_ingestion] >> end