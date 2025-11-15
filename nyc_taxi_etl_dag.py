from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'nyc_taxi_etl_dag',
    default_args=default_args,
    description='ETL from GCS CSVs to BigQuery via Dataflow',
    schedule_interval='@daily',
    start_date=datetime(2025, 10, 27),
    catchup=False,
    max_active_runs=1
) as dag:

    run_dataflow = BashOperator(
        task_id='run_dataflow',
        bash_command=(
            "python3 /home/airflow/gcs/dags/dataflow_etl.py "
            "--runner=DataflowRunner "
            "--project=etl-demo-pipeline "
            "--region=us-central1 "
            "--temp_location=gs://etl-nyctaxi-bucket/temp/ "
            "--input=gs://etl-nyctaxi-bucket/nyc_taxi/*.csv "
            "--output_table=etl-demo-pipeline:nyctaxi_dataset.yellow_tripdata_all "
        )
    )

    run_dataflow
