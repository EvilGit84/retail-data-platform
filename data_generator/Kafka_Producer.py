from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers='127.0.0.1:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

orders =[
    {'order_id': 101, 'amount': 300, 'city': 'pune'},
    {'order_id': 102, 'amount': 150, 'city': 'mumbai'}
]
for order in orders:
    producer.send('retail_orders', order)
    print(f"Sent order: {order}")
    time.sleep(1)
producer.flush()