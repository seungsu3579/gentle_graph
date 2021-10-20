import sys
sys.path.append("/home/capje/pororo/")

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
    'test_data_create',
    default_args=default_args,
    description='Random Forest 용 데이터 생성 프로세스',
    schedule_interval='0 * * * *',
    start_date=datetime(2021,5,23, 0),
    tags=['analysis', 'test'],
)


# [task]

############### crawling #################

crawling_naver_A = BashOperator(
    task_id='crawl_naver_news_A',
    depends_on_past=False,
    bash_command='python3 /home/capje/crawling/navernews_crawlerA.py',
    retries=3,
    dag=dag,
)

crawling_naver_B = BashOperator(
    task_id='crawl_naver_news_B',
    depends_on_past=False,
    bash_command='python3 /home/capje/crawling/navernews_crawlerB.py',
    retries=3,
    dag=dag,
)

crawling_naver_C = BashOperator(
    task_id='crawl_naver_news_C',
    depends_on_past=False,
    bash_command='python3 /home/capje/crawling/navernews_crawlerC.py',
    retries=3,
    dag=dag,
)

kill_chrome_process = BashOperator(
    task_id='kill_chrome_process',
    depends_on_past=False,
    bash_command='/home/capje/airflow/kill_oldest_chrome.sh',
    retries=3,
    dag=dag,
)

############### preprocess #################

preprocess_naver = BashOperator(
    task_id='preprocess_naver',
    depends_on_past=False,
    bash_command='python3 /home/capje/pororo/preprocessor.py naver_news',
    retries=3,
    dag=dag,
)


############### insert data into neo4j #################

insert_neo4j_naver_news_test = BashOperator(
    task_id='insert_neo4j_naver_news_test',
    depends_on_past=False,
    bash_command='python3 /home/capje/neo4j_query/neo4j_insert.py naver_news_test',
    retries=3,
    dag=dag,
)


# [dependency]
[crawling_naver_A, crawling_naver_B, crawling_naver_C] >> preprocess_naver >> insert_neo4j_naver_news_test


# [crawling_naver_A, crawling_naver_B, crawling_naver_C] >> kill_chrome_process
