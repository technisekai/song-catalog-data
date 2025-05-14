from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from datetime import datetime
from cores.auth import clickhouse_conn

# DAG Configuration
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False
}

def clickhouse_execute_sql(
        dwh_host: str,
        dwh_port: str,
        dwh_username: str,
        dwh_password: str,
        dwh_database: str,
        sql_file: str
):
    # Create clickhouse connection
    ch_conn = clickhouse_conn(
        host=dwh_host,
        port=dwh_port,
        username=dwh_username,
        password=dwh_password,
        database=dwh_database
    )
    # Open query
    file = open(sql_file)
    query = file.read()
    file.close()
    # Execute query
    ch_conn.command(query)
    # Close clickhouse connection
    ch_conn.close()


with DAG(
    "songs_catalaog_silver",
    default_args=default_args,
    description="",
    schedule_interval=None,
    start_date=datetime(2025, 4, 4),
    catchup=False,
) as dag:
    # Declare variables
    dwh_creds = Variable.get(key="dwh-creds-secret", deserialize_json=True)

    start = DummyOperator(task_id="start", dag=dag)

    end = DummyOperator(task_id="end", dag=dag)

    create_table_task = PythonOperator(
        task_id=f"create_table_if_not_exists",
        python_callable=clickhouse_execute_sql,
        op_kwargs={
            "dwh_host": dwh_creds["dwh_host"],
            "dwh_port": dwh_creds["dwh_port"],
            "dwh_username": dwh_creds["dwh_username"],
            "dwh_password": dwh_creds["dwh_password"],
            "dwh_database": "silver",
            "sql_file": "/opt/airflow/dags/payloads/sql/silver/schemas_songs_catalog.sql"
        }
    )

    ingest_data_task = PythonOperator(
        task_id=f"ingest_data_to_silver",
        python_callable=clickhouse_execute_sql,
        op_kwargs={
            "dwh_host": dwh_creds["dwh_host"],
            "dwh_port": dwh_creds["dwh_port"],
            "dwh_username": dwh_creds["dwh_username"],
            "dwh_password": dwh_creds["dwh_password"],
            "dwh_database": "silver",
            "sql_file": "/opt/airflow/dags/payloads/sql/silver/incremental_songs_catalog.sql"
        }
    )

    start >> create_table_task >> ingest_data_task >> end