from kafka import KafkaConsumer
import json
import psycopg2
from datetime import datetime

#kafka consumer to read data from kafka topic and insert into postgres
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
def validate_order(order):
    required_fields = ['order_id', 'amount', 'city', 'created_at']

    for field in required_fields:
        if field not in order:
            return False

    if not isinstance(order['order_id'], int):
        return False

    if not isinstance(order['amount'], (int, float)) or order['amount'] <= 0:
        return False

    if not isinstance(order['city'], str):
        return False

    return True


for message in consumer:
    order = message.value
     # Add current timestamp to the order
    order['created_at'] = datetime.now()
    print("Received:", order)


    cursor.execute(
    """
    INSERT INTO retail_orders (order_id, amount, city, created_at)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (order_id) DO NOTHING;
    """,
    (order['order_id'], order['amount'], order['city'], order['created_at'])
)
    conn.commit()
    print("Inserted order into PostgreSQL:", order) 
