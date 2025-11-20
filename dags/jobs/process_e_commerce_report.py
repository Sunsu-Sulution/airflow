import time, os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.sdk import task
from util.handler_error import handle_error
from util.lark_helper import get_lark_base_to_df
from util.s3 import S3Client
import pandas as pd


time.tzset()
schedule = "0 * * * *"


with DAG(
    dag_id="e_commerce_report",
    description="e-commerce report",
    start_date=datetime.now() - timedelta(days=1),
    tags=["e-commerce", "report"],
    schedule=schedule,
) as dag:
    s3_client = S3Client()
    selected_dt = datetime.now() - timedelta(days=1)
    month_str = f"{selected_dt.month:02d}"
    day_str = f"{selected_dt.day:02d}"

    @task
    def process_e_commerce_lazada_report():
        category_mapping_df = get_lark_base_to_df(os.getenv("E_COMMERCE_BASE_APP_ID"), os.getenv("E_COMMERCE_BASE_TABLE_ID"), {
            "conditions": [
                {
                    "field_name": "sku",
                    "operator": "isNotEmpty",
                    "value": []
                }
            ],
            "conjunction": "and"
        })

        print(category_mapping_df)
        prefix = f"lazada/orders/{selected_dt.year}/{month_str}/{day_str}"
        keys_iter = s3_client.list_s3_keys(prefix=prefix)
        # filtered = pd.Series(
        #     filter_keys(keys_iter, must_end_with_json=True)
        # )
        # files_df = pd.DataFrame(filtered, columns=["file name"])

    process_e_commerce_lazada_report()
