import os
import json
import signal
import sys
from datetime import datetime
from kafka import KafkaConsumer
from botocore.exceptions import ClientError
import boto3
from dotenv import load_dotenv

# ------------------------------------------------------------------------------
# Load environment variables
# ------------------------------------------------------------------------------
load_dotenv()

MINIO_BUCKET = os.getenv("MINIO_BUCKET")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
KAFKA_BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", 10))

# ------------------------------------------------------------------------------
# MinIO (S3) client
# ------------------------------------------------------------------------------
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    region_name="us-east-1",
)

# ------------------------------------------------------------------------------
# Ensure bucket exists
# ------------------------------------------------------------------------------
try:
    s3.head_bucket(Bucket=MINIO_BUCKET)
    print(f"🪣 BUCKET | '{MINIO_BUCKET}' already exists")
except ClientError as e:
    if e.response["Error"]["Code"] == "404":
        s3.create_bucket(Bucket=MINIO_BUCKET)
        print(f"🪣 BUCKET | Created '{MINIO_BUCKET}'")
    else:
        raise

# ------------------------------------------------------------------------------
# Kafka Consumer
# ------------------------------------------------------------------------------
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVER],
    group_id=KAFKA_GROUP_ID,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

# ------------------------------------------------------------------------------
# Pretty printing helpers
# ------------------------------------------------------------------------------
def pretty_event(event):
    return (
        f"🎧 EVENT | "
        f"{event['event_type']:<12} | "
        f"{event['song_name']:<18} | "
        f"{event['artist_name']:<14} | "
        f"user={event['user_id'][:6]} | "
        f"{event['country']} | "
        f"{event['device_type']}"
    )

def pretty_batch(size):
    return f"📦 BATCH | collected={size}/{BATCH_SIZE}"

def pretty_upload(path, count):
    return f"☁️  UPLOAD | events={count} | {path}"

# ------------------------------------------------------------------------------
# Graceful shutdown
# ------------------------------------------------------------------------------
running = True

def shutdown(sig, frame):
    global running
    print("\n🛑 Shutdown signal received. Closing consumer...")
    running = False
    consumer.close()
    print("✅ Consumer closed")
    sys.exit(0)

signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)

# ------------------------------------------------------------------------------
# Main loop
# ------------------------------------------------------------------------------
print("🎧 Kafka → MinIO Consumer started")
print(f"➡ Topic   : {KAFKA_TOPIC}")
print(f"➡ Group   : {KAFKA_GROUP_ID}")
print(f"➡ Bucket  : {MINIO_BUCKET}")
print("-" * 80)

batch = []

for message in consumer:
    if not running:
        break

    event = message.value
    batch.append(event)

    # Print each event (like producer)
    print(pretty_event(event))
    print(pretty_batch(len(batch)))

    if len(batch) >= BATCH_SIZE:
        now = datetime.utcnow()
        date_path = now.strftime("date=%Y-%m-%d/hour=%H")
        file_name = f"spotify_events_{now.strftime('%Y-%m-%dT%H-%M-%S')}.json"
        file_path = f"bronze/{date_path}/{file_name}"

        body = "\n".join(json.dumps(e) for e in batch)

        try:
            s3.put_object(
                Bucket=MINIO_BUCKET,
                Key=file_path,
                Body=body.encode("utf-8"),
            )
            print(pretty_upload(file_path, len(batch)))
        except Exception as e:
            print(f"❌ ERROR | Failed to upload batch → {e}")

        batch.clear()
        print("-" * 80)


# import json
# import os
# from kafka import KafkaConsumer
# from datetime import datetime
# import boto3
# from dotenv import load_dotenv

# # ---------- Load environment variables ----------
# load_dotenv()

# # ---------- Configuration ----------
# MINIO_BUCKET = os.getenv("MINIO_BUCKET")
# MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
# MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
# MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
# KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
# KAFKA_BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER")
# KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID")
# BATCH_SIZE = int(os.getenv("BATCH_SIZE", 10)) 

# # ---------- Connect to MinIO ----------
# s3 = boto3.client(
#     "s3",
#     endpoint_url=MINIO_ENDPOINT,
#     aws_access_key_id=MINIO_ACCESS_KEY,
#     aws_secret_access_key=MINIO_SECRET_KEY
# )

# # Ensure bucket exists (idempotent)
# try:
#     s3.head_bucket(Bucket=MINIO_BUCKET)
#     print(f"Bucket {MINIO_BUCKET} already exists.")
# except Exception:
#     s3.create_bucket(Bucket=MINIO_BUCKET)
#     print(f"Created bucket {MINIO_BUCKET}.")

# # ---------- Kafka Consumer Setup ----------
# consumer = KafkaConsumer(
#     KAFKA_TOPIC,
#     bootstrap_servers=[KAFKA_BOOTSTRAP_SERVER],
#     auto_offset_reset="earliest",
#     enable_auto_commit=True,
#     group_id=KAFKA_GROUP_ID,
#     value_deserializer=lambda v: json.loads(v.decode("utf-8"))
# )

# print(f"🎧 Listening for events on Kafka topic '{KAFKA_TOPIC}'...")

# batch = []

# for message in consumer:
#     event = message.value
#     batch.append(event)

#     if len(batch) >= BATCH_SIZE:
#         now = datetime.utcnow()
#         date_path = now.strftime("date=%Y-%m-%d/hour=%H")
#         file_name = f"spotify_events_{now.strftime('%Y-%m-%dT%H-%M-%S')}.json"
#         file_path = f"bronze/{date_path}/{file_name}"

#         json_data = "\n".join([json.dumps(e) for e in batch])

#         s3.put_object(
#             Bucket=MINIO_BUCKET,
#             Key=file_path,
#             Body=json_data.encode("utf-8")
#         )

#         print(f"✅ Uploaded {len(batch)} events to MinIO: {file_path}")
#         batch = []