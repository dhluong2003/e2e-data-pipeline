from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, time, timedelta

dag = DAG(
    dag_id='daily_dag',
    start_date=datetime(2021, 1, 1),
    schedule_interval='0 7 * * *'
)
with dag:
    # generate 1 million records and send to kafka
    run_script = BashOperator(
        task_id='run_script',
        bash_command='cd scripts && python ecom_log_gen_kafka.py --kafka kafka:9092 --count 1000000 --topic ecom_json',
    )



