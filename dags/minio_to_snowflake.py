import json
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import boto3
import snowflake.connector
from dotenv import load_dotenv

# ------------------------------------------------------
# Load environment variables
# ------------------------------------------------------
load_dotenv(dotenv_path="/opt/airflow/dags/.env")

# ----- MinIO Configuration -----
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")
MINIO_PREFIX = os.getenv("MINIO_PREFIX", "")

# ----- Snowflake Configuration -----
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_TABLE = os.getenv("SNOWFLAKE_TABLE")

# ----- Local Temp Path -----
LOCAL_TEMP_PATH = os.getenv("LOCAL_TEMP_PATH", "/tmp/spotify_raw.json")

# ------------------------------------------------------
# Pretty logging helpers
# ------------------------------------------------------
def pretty_minio_file(key):
    return f"🪣 MINIO FILE | {key}"

def pretty_event(event):
    return (
        f"🎵 EVENT | {event['event_type']:<12} | "
        f"{event['song_name']:<18} | {event['artist_name']:<14} | "
        f"user={event['user_id'][:6]} | {event['country']} | {event['device_type']}"
    )

def pretty_summary(total):
    return f"☁️  UPLOAD | Total events loaded: {total} into Snowflake table: {SNOWFLAKE_TABLE}"


# ------------------------------------------------------
# Extract from MinIO
# ------------------------------------------------------
def extract_from_minio():
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )

    response = s3.list_objects_v2(Bucket=MINIO_BUCKET, Prefix=MINIO_PREFIX)
    contents = response.get("Contents", [])

    all_events = []
    for obj in contents:
        key = obj["Key"]
        if not key.endswith(".json"):
            continue
        print(pretty_minio_file(key))

        data = s3.get_object(Bucket=MINIO_BUCKET, Key=key)
        lines = data["Body"].read().decode("utf-8").splitlines()
        for line in lines:
            try:
                all_events.append(json.loads(line))
            except json.JSONDecodeError:
                print(f"❌ Failed to decode line in {key}")

    with open(LOCAL_TEMP_PATH, "w") as f:
        json.dump(all_events, f)

    print(f"✅ Extracted {len(all_events)} events from MinIO → {LOCAL_TEMP_PATH}")
    return LOCAL_TEMP_PATH


# ------------------------------------------------------
# Load raw data to Snowflake
# ------------------------------------------------------
def load_raw_to_snowflake(**context):
    file_path = context["ti"].xcom_pull(task_ids="extract_data")

    with open(file_path, "r") as f:
        events = json.load(f)

    if not events:
        print("⚠️ No events found to load.")
        return

    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )
    cur = conn.cursor()

    # Create table if not exists
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {SNOWFLAKE_TABLE} (
        event_id STRING,
        user_id STRING,
        song_id STRING,
        artist_name STRING,
        song_name STRING,
        event_type STRING,
        device_type STRING,
        country STRING,
        timestamp STRING
    );
    """
    cur.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
    cur.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")
    cur.execute(create_table_sql)

    # Insert events
    insert_sql = f"""
        INSERT INTO {SNOWFLAKE_TABLE} (
            event_id, user_id, song_id, artist_name, song_name,
            event_type, device_type, country, timestamp
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    for event in events:
        cur.execute(insert_sql, (
            event.get("event_id"),
            event.get("user_id"),
            event.get("song_id"),
            event.get("artist_name"),
            event.get("song_name"),
            event.get("event_type"),
            event.get("device_type"),
            event.get("country"),
            event.get("timestamp"),
        ))
        # Print each event (pretty)
        print(pretty_event(event))

    conn.commit()
    cur.close()
    conn.close()

    print(pretty_summary(len(events)))


# ------------------------------------------------------
# Airflow DAG
# ------------------------------------------------------
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 10, 21),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "spotify_minio_to_snowflake_bronze_pretty",
    default_args=default_args,
    description="Load raw Spotify events from MinIO to Snowflake Bronze table with pretty logs",
    schedule_interval="@hourly",
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_from_minio
    )

    load_task = PythonOperator(
        task_id="load_raw_to_snowflake",
        python_callable=load_raw_to_snowflake,
        provide_context=True
    )

    extract_task >> load_task
