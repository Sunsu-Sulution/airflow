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
import pandas as pd

time.tzset()
schedule = "0 6 * * *"


with DAG(
    dag_id="migrate_databae",
    description="Process Migrate Data",
    start_date=datetime.now() - timedelta(days=1),
    tags=["migrate"],
    schedule=schedule,
) as dag:

    @task
    def migrate_rocket_data():
        return
        df = pd.read_csv("/downloads/ContactList_Bearhouse_20241105.csv")

        df["phone_no"] = df["MEMBER_TEL"] - 66000000000
        df["fullname"] = df["MEMBER_FULL_NAME"]
        df["tier_name"] = df["MEMBER_TIER_NAME"]
        df["current_point"] = df["CONTACT_POINTS_BALANCE"]
        df["birthdate"] = df["DATEOFBIRTH"]
        df["register_date"] = df["REGISTER_DATE"]
        df["last_login_date"] = df["LAST_LOGIN_DATE"]
        df["last_activity_date"] = df["LAST_ACTIVITY_DATE"]

        df = df[["phone_no", "fullname", "tier_name", "current_point", "birthdate", "register_date", "last_login_date", "last_activity_date"]]

        print(df)
        database_url = os.environ.get("DATABASE_URL")
        table_name = "migrate_rocket_members"
        engine = create_engine(database_url)

        try:
            df.to_sql(name=table_name, con=engine, if_exists="append", index=False, method="multi", chunksize=1000)
            print(f"Wrote {len(df)} rows to {table_name}")
        finally:
            engine.dispose()


    @task
    def migrate_food_story_data():
        return
        df = pd.read_csv("/downloads/users_20251103-161907.csv")
        print(df.dtypes.to_frame("dtype"))
        df = df[["phone_no", "firstname_th", "lastname_th", "firstname_en", "lastname_en", "current_point", "tier_id", "birth_date", "tier_entry_date", "created_date", "updated_date"]]

        tier_name_mapping = ["", "First Meet", "Friend", "Buddy", "Partner", "Crew", "Team", "CEO"]

        def get_mapping(idx):
            try:
                if pd.isna(idx):
                    return None
                i = int(float(idx))
                if 0 <= i < len(tier_name_mapping):
                    return tier_name_mapping[i]
                return None
            except (ValueError, TypeError):
                return None

        df["tier_name"] = df["tier_id"].apply(get_mapping)

        print(df)
        database_url = os.environ.get("DATABASE_URL")
        table_name = "migrate_food_story_members"
        engine = create_engine(database_url)

        try:
            df.to_sql(name=table_name, con=engine, if_exists="append", index=False, method="multi", chunksize=1000)
            print(f"Wrote {len(df)} rows to {table_name}")
        finally:
            engine.dispose()

    migrate_food_story_data() >> migrate_rocket_data()
