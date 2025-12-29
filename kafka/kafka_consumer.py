from kafka import KafkaConsumer
import json
import psycopg2

# PostgreSQL connection
conn = psycopg2.connect(
    host="localhost",
    database="retail_db",
    user="retail_user",
    password="retail_password"
)
cursor = conn.cursor()

consumer = KafkaConsumer(
    'retail_orders',
    bootstrap_servers='127.0.0.1:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='retail-consumer-group',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print("Consumer started, waiting for messages...")

for message in consumer:
    order = message.value
    print("Received:", order)

    cursor.execute(
        """
        INSERT INTO retail_orders (order_id, amount, city)
        VALUES (%s, %s, %s)
        """,
        (order['order_id'], order['amount'], order['city'])
    )
    conn.commit()
