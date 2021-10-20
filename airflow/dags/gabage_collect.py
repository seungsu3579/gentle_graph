from datetime import timedelta
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
# from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago


# [args]
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['hwa0327@yonsei.ac.kr'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}


# [DAG]
dag = DAG(
    'Gabage_Collector',
    default_args=default_args,
    description='kill finished chrome process and remove tmp file',
    schedule_interval='50 * * * *',
    start_date=datetime(2021,5,26, 6),
    tags=['gabage_collector'],
)

# [task]

kill_chrome_process = BashOperator(
    task_id='kill_chrome_process',
    depends_on_past=False,
    bash_command="/home/capje/airflow/kill_oldest_chrome.sh ",
    retries=3,
    dag=dag,
)

flush_tmp_dir = BashOperator(
    task_id='flush_tmp_directory',
    depends_on_past=False,
    bash_command="/home/capje/airflow/remove_chrome_tmp.sh ",
    retries=3,
    dag=dag,
)


# [dependency]