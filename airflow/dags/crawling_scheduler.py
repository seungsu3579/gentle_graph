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
    'crawler_test',
    default_args=default_args,
    description='Data ETL Process',
    schedule_interval='0 * * * *',
    start_date=datetime(2021,5,26, 6),
    tags=['crawler', 'test'],
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

crawling_daum = BashOperator(
    task_id='crawl_daum_news',
    depends_on_past=False,
    bash_command='python3 /home/capje/crawling/daumnews_crawler.py',
    retries=3,
    dag=dag,
)

crawling_youtube_A = BashOperator(
    task_id='crawl_youtube_contents_A',
    depends_on_past=False,
    bash_command='python3 /home/capje/crawling/youtube_crawler_A.py',
    retries=3,
    dag=dag,
)

crawling_youtube_B = BashOperator(
    task_id='crawl_youtube_contents_B',
    depends_on_past=False,
    bash_command='python3 /home/capje/crawling/youtube_crawler_B.py',
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


preprocess_daum = BashOperator(
    task_id='preprocess_daum',
    depends_on_past=False,
    bash_command='python3 /home/capje/pororo/preprocessor.py daum_news',
    retries=3,
    dag=dag,
)

preprocess_youtube = BashOperator(
    task_id='preprocess_youtube',
    depends_on_past=False,
    bash_command='python3 /home/capje/pororo/preprocessor.py youtube_contents',
    retries=3,
    dag=dag,
)


############### insert data into neo4j #################

insert_neo4j_naver_news = BashOperator(
    task_id='insert_neo4j_naver_news',
    depends_on_past=False,
    bash_command='python3 /home/capje/neo4j_query/neo4j_insert.py naver_news',
    retries=3,
    dag=dag,
)

insert_neo4j_daum_news = BashOperator(
    task_id='insert_neo4j_daum_news',
    depends_on_past=False,
    bash_command='python3 /home/capje/neo4j_query/neo4j_insert.py daum_news',
    retries=3,
    dag=dag,
)

insert_neo4j_youtube_contents = BashOperator(
    task_id='insert_neo4j_youtube_contents',
    depends_on_past=False,
    bash_command='python3 /home/capje/neo4j_query/neo4j_insert.py youtube_contents',
    retries=3,
    dag=dag,
)

centrality_calculate = BashOperator(
    task_id='centrality_calculate',
    depends_on_past=False,
    bash_command='python3 /home/capje/neo4j_query/neo4j_insert.py centrality',
    retries=3,
    dag=dag,
)

feature_importance = BashOperator(
    task_id='feature_importance',
    depends_on_past=False,
    bash_command='python3 /home/capje/neo4j_query/neo4j_insert.py feature_importance',
    retries=3,
    dag=dag,
)

keyword_info = BashOperator(
    task_id='keyword_info',
    depends_on_past=False,
    bash_command='python3 /home/capje/neo4j_query/neo4j_insert.py keyword_info',
    retries=3,
    dag=dag,
)

# [dependency]
[crawling_naver_A, crawling_naver_B, crawling_naver_C] >> preprocess_naver >> insert_neo4j_naver_news
crawling_daum >> preprocess_daum >> insert_neo4j_daum_news
[crawling_youtube_A, crawling_youtube_B] >> preprocess_youtube >> insert_neo4j_youtube_contents


[insert_neo4j_naver_news, insert_neo4j_daum_news, insert_neo4j_youtube_contents] >> centrality_calculate >> feature_importance >> keyword_info