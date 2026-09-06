from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="hacker_news_pipeline",
    start_date=datetime(2026, 9, 4),
    schedule="@hourly",
    catchup=False,
    tags=["hacker-news"],
) as dag:

    extract_hacker_news = BashOperator(
        task_id="extract_hacker_news",
        bash_command="python /opt/airflow/dags/extract_hn.py",
    )

    clean_hacker_news = BashOperator(
        task_id="clean_hacker_news",
        bash_command="python /opt/airflow/dags/spark_clean.py",
    )

    extract_hacker_news >> clean_hacker_news