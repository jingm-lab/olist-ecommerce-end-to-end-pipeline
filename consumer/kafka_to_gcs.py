from google.cloud import storage
from kafka import KafkaConsumer
import json
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv
from collections import defaultdict

# -----------------------------
# Load secrets from .env
# -----------------------------
load_dotenv()

# Kafka consumer settings
consumer = KafkaConsumer(
    "ecommerce_server.public.customers",
    "ecommerce_server.public.geolocations",
    "ecommerce_server.public.order_items",
    "ecommerce_server.public.order_payments",
    "ecommerce_server.public.order_reviews",
    "ecommerce_server.public.orders",
    "ecommerce_server.public.product_category_name_translation",
    "ecommerce_server.public.products",
    "ecommerce_server.public.sellers",
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP"),
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    group_id="gcs-writer",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    consumer_timeout_ms=30000,
)

# GCS client
storage_client = storage.Client()

bucket_name = os.getenv("GCS_BUCKET")
bucket = storage_client.bucket(bucket_name)

if not bucket.exists():
    bucket = storage_client.create_bucket(bucket_name)
    print(f"Created bucket {bucket_name}")
else:
    print(f"Bucket {bucket_name} already exists")


# Consume and write function
def write_to_gcs(table_name, records):
    if not records:
        return
    df = pd.DataFrame(records)
    date_str = datetime.now().strftime("%Y-%m-%d")
    parquet_bytes = df.to_parquet(engine="pyarrow", index=False)
    gcs_key = f'{table_name}/date={date_str}/{table_name}_{datetime.now().strftime("%H%M%S%f")}.parquet'
    blob = bucket.blob(gcs_key)
    blob.upload_from_string(parquet_bytes, content_type="application/octet-stream")
    print(f"✅ Uploaded {len(records)} records to gcs://{bucket}/{gcs_key}")


# Batch consume
batch_size = 1000
buffer = defaultdict(list)

print("✅ Connected to Kafka. Listening for messages...")

for message in consumer:
    topic = message.topic
    event = message.value
    payload = event.get("payload", {})
    record = payload.get("after")  # Only take the actual row

    if record:
        for key, value in record.items():
            if isinstance(value, (int, float)) and abs(value) > 9.46e14:
                record[key] = pd.to_datetime(value, unit="us")
        buffer[topic].append(record)
        print(f"[{topic}] -> {record}")  # Debugging

    if len(buffer[topic]) >= batch_size:
        write_to_gcs(topic.split(".")[-1], buffer[topic])
        buffer[topic] = []
        consumer.commit()

for topic, record in buffer.items():
    if record:
        write_to_gcs(topic.split(".")[-1], record)
        buffer[topic] = []
        consumer.commit()
