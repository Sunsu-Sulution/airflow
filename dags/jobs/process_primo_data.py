from datetime import datetime, timedelta
from airflow import DAG
from airflow.sdk import task
from time import tzset
from util.s3 import S3Client
from util.chrome import Chrome
from sqlalchemy import create_engine
from util.format import to_datetime_series, to_str_id_like
import pandas as pd
import os

tzset()
schedule = "9 0 * * *"


with DAG(
    dag_id="process_primo_data",
    description="Process Primo Data",
    start_date=datetime.now() - timedelta(days=1),
    tags=["Primo"],
    schedule=schedule,
) as dag:
    s3_client = S3Client()
    now = datetime.today()
    yesterday = now - timedelta(days=1)
    bucket_name = "bearhouse-crm-primo"

    @task()
    def process_primo_memberships():
        prefix = (
            f"prod/report/"
            f"{yesterday.year}/"
            f"{yesterday.month:02d}/"
            f"{yesterday.day:02d}/"
            f"incremental/report_memberships/"
        )

        df = s3_client.read_csv_from_folder(bucket_name, prefix)
        print(df.dtypes.to_frame("dtype"))

        database_url = os.environ.get("DATABASE_URL")
        table_name = "primo_memberships"
        engine = create_engine(database_url)

        date_cols = ["date_of_birth", "last_active_at", "created_at", "updated_at"]
        str_cols = [
            "citizen_id",
            "passport_no",
            "mobile",
            "post_code",
            "address_freetext",
            "room_no",
            "building_village",
            "moo",
            "road",
            "soi",
            "granted_tc_version",
        ]

        for c in date_cols:
            if c in df.columns:
                df[c] = to_datetime_series(df[c], dayfirst=True)

        for c in str_cols:
            if c in df.columns:
                df[c] = df[c].apply(to_str_id_like)

        if "post_code" in df.columns:
            df["post_code"] = df["post_code"].astype("string")

        try:
            df.to_sql(name=table_name, con=engine, if_exists="append", index=False, method="multi", chunksize=1000)
            print(f"Wrote {len(df)} rows to {table_name}")
        finally:
            engine.dispose()

    @task()
    def process_primo_coupons():
        prefix = (
            f"prod/report/"
            f"{yesterday.year}/"
            f"{yesterday.month:02d}/"
            f"{yesterday.day:02d}/"
            f"incremental/report_coupons/"
        )

        df = s3_client.read_csv_from_folder(bucket_name, prefix)
        print(df.dtypes.to_frame("dtype"))

        database_url = os.environ.get("DATABASE_URL")
        table_name = "primo_coupons"
        engine = create_engine(database_url)

        date_cols = ["expired_at", "redeemed_at", "used_at", "updated_at"]
        str_cols = [
            "owner",
            "transaction_id",
            "coupon_outcome_id",
            "coupon_code",
            "coupon_type",
            "coupon_status",
            "coupon_collections",
            "customer_ref",
            "campaign_code",
            "campaign",
            "brand",
            "acceptance_type",
            "outcome_th",
            "point_currency",
            "ref_code",
        ]

        for c in date_cols:
            if c in df.columns:
                df[c] = to_datetime_series(df[c], dayfirst=True)

        for c in str_cols:
            if c in df.columns:
                df[c] = df[c].apply(to_str_id_like)

        try:
            df.to_sql(name=table_name, con=engine, if_exists="append", index=False, method="multi",
                      chunksize=1000)
            print(f"Wrote {len(df)} rows to {table_name}")
        finally:
            engine.dispose()

    @task()
    def process_primo_member_tier_movement():
        prefix = (
            f"prod/report/"
            f"{yesterday.year}/"
            f"{yesterday.month:02d}/"
            f"{yesterday.day:02d}/"
            f"incremental/report_member_tier_movement/"
        )

        df = s3_client.read_csv_from_folder(bucket_name, prefix)
        print(df.dtypes.to_frame("dtype"))

        database_url = os.environ.get("DATABASE_URL")
        table_name = "primo_member_tier_movement"
        engine = create_engine(database_url)

        date_cols = ["created_at", "updated_at", "entry_date", "expired_date"]
        str_cols = [
            "log_id",
            "customer_ref",
            "loyalty_program_name",
            "tier_group_name",
            "tier_name",
            "previous_tier_name",
            "owner",
        ]

        for c in date_cols:
            if c in df.columns:
                df[c] = to_datetime_series(df[c], dayfirst=True)

        for c in str_cols:
            if c in df.columns:
                df[c] = df[c].apply(to_str_id_like)

        try:
            df.to_sql(name=table_name, con=engine, if_exists="append", index=False, method="multi",
                      chunksize=1000)
            print(f"Wrote {len(df)} rows to {table_name}")
        finally:
            engine.dispose()

    @task()
    def process_primo_member_point_on_hand():
        prefix = (
            f"prod/report/"
            f"{yesterday.year}/"
            f"{yesterday.month:02d}/"
            f"{yesterday.day:02d}/"
            f"fulldump/report_member_point_on_hand/"
        )

        df = s3_client.read_csv_from_folder(bucket_name, prefix)
        print(df.dtypes.to_frame("dtype"))

        database_url = os.environ.get("DATABASE_URL")
        table_name = "primo_point_on_hand"
        engine = create_engine(database_url)

        date_cols = ["expired_date", "updated_at", "created_at"]
        str_cols = [
            "transaction_id",
            "customer_ref",
            "point_currency",
            "point_type",
            "member_status",
            "owner",
        ]

        for c in date_cols:
            if c in df.columns:
                df[c] = to_datetime_series(df[c], dayfirst=True)

        for c in str_cols:
            if c in df.columns:
                df[c] = df[c].apply(to_str_id_like)

        try:
            df.to_sql(name=table_name, con=engine, if_exists="replace", index=False, method="multi",
                      chunksize=1000)
            print(f"Wrote {len(df)} rows to {table_name}")
        finally:
            engine.dispose()

    process_primo_memberships() >> process_primo_coupons() >> process_primo_member_tier_movement() >> process_primo_member_point_on_hand()
