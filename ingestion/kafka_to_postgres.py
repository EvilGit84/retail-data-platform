from kafka import KafkaConsumer
import json
import psycopg2
from datetime import datetime

# Kafka consumer
consumer = KafkaConsumer(
    'retail_orders',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='retail-postgres-consumer',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

# PostgreSQL connection
conn = psycopg2.connect(
    host="localhost",
    database="retail_db",
    user="retail_user",
    password="retail_password",
    port=5432
)
cursor = conn.cursor()

# ---------- Validation & Cleaning ----------
def validate_and_clean(order):
    # Required fields
    if 'order_id' not in order or 'amount' not in order or 'city' not in order:
        return None

    # Type checks
    if not isinstance(order['order_id'], int):
        return None

    if not isinstance(order['amount'], (int, float)) or order['amount'] <= 0:
        return None

    if not isinstance(order['city'], str):
        return None

    # Cleaning
    order['city'] = order['city'].strip().lower()

    # Add ingestion timestamp
    order['created_at'] = datetime.utcnow()

    return order

print("Kafka → PostgreSQL ingestion service started...")

# ---------- Consume & Insert ----------
for message in consumer:
    raw_order = message.value
    print("Received raw:", raw_order)

    order = validate_and_clean(raw_order)

    if order is None:
        print("Invalid record skipped")
        continue

    try:
        cursor.execute(
            """
            INSERT INTO retail_orders (order_id, amount, city, created_at)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (order_id) DO NOTHING;
            """,
            (
                order['order_id'],
                order['amount'],
                order['city'],
                order['created_at']
            )
        )
        conn.commit()
        print("Inserted:", order)

    except Exception as e:
        conn.rollback()
        print("DB error:", e)
