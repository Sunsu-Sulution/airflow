import time
from datetime import datetime, timedelta
from airflow import DAG
from airflow.sdk import task
from util.handler_error import handle_error


time.tzset()
schedule = "0 * * * *"


with DAG(
    dag_id="health_check",
    description="Health Check",
    start_date=datetime.now() - timedelta(days=1),
    tags=["Health Check"],
    schedule=schedule,
) as dag:
    now = datetime.today()

    @task
    def process_health_check():
        print(f"health check started form {now}")
        print(f"health check done")

    process_health_check()
