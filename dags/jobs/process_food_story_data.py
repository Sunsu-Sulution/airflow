import time
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.sdk import task
from sqlalchemy import create_engine
from util.quicksight import get_food_story_bills_detail, get_food_story_promotions
from util.chrome import Chrome
from util.s3 import S3Client
from util.chrome import Chrome
from util.format import to_datetime_series, to_str_id_like

time.tzset()
schedule = "0 20 * * *"


with DAG(
    dag_id="process_food_story_data",
    description="Process Food Story Data",
    start_date=datetime.now() + timedelta(days=1),
    tags=["Food Story"],
    schedule=schedule,
) as dag:
    now = datetime.today()
    yesterday = now - timedelta(days=1)

    @task
    def process_bill_detail():
        df = get_food_story_bills_detail(yesterday)
        print(df)

        database_url = os.environ.get("DATABASE_URL")
        table_name = "food_story_bill_detail"
        engine = create_engine(database_url)

        date_cols = ["payment_date"]
        str_cols = [
            "receipt_no",
            "tax_inv_no",
            "cus_crm_member_id",
            "customer_phone_number",
            "customer_name",
            "branch_code",
            "store_name",
            "payment_type",
            "order_type",
        ]

        for c in date_cols:
            if c in df.columns:
                df[c] = to_datetime_series(df[c], dayfirst=True)

        for c in str_cols:
            if c in df.columns:
                df[c] = df[c].apply(to_str_id_like)

        df["void"] = df["void"] == "Y"
        df["include_revenue"] = df["include_revenue"] == "Y"

        if "payment_date" in df.columns and "payment_time" in df.columns:
            df["payment_datetime"] = df.apply(
                lambda x: datetime.combine(
                    x["payment_date"].date(),
                    datetime.strptime(x["payment_time"], "%H:%M:%S").time()
                ),
                axis=1
            )

        try:
            df.to_sql(name=table_name, con=engine, if_exists="append", index=False, method="multi", chunksize=1000)
            print(f"Wrote {len(df)} rows to {table_name}")
        finally:
            engine.dispose()

    @task
    def process_promotions():
        df = get_food_story_promotions(yesterday)
        print(df)

        database_url = os.environ.get("DATABASE_URL")
        table_name = "food_story_promotion"
        engine = create_engine(database_url)

        date_cols = ["payment_date"]
        str_cols = [
            "receipt_no",
            "tax_inv_no",
            "promotion_type",
            "final_pro_ref_code",
            "promotion_name",
            "branch_code",
            "store_name",
            "payment_type",
            "order_type",
            "invoice_item_id",
        ]

        for c in date_cols:
            if c in df.columns:
                df[c] = to_datetime_series(df[c], dayfirst=True)

        for c in str_cols:
            if c in df.columns:
                df[c] = df[c].apply(to_str_id_like)

        df["void"] = df["void"] == "Y"

        try:
            df.to_sql(name=table_name, con=engine, if_exists="append", index=False, method="multi", chunksize=1000)
            print(f"Wrote {len(df)} rows to {table_name}")
        finally:
            engine.dispose()

    process_bill_detail() >> process_promotions()
