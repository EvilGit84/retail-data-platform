from kafka import KafkaProducer
import json
import time
import random


producer = KafkaProducer(
    bootstrap_servers='127.0.0.1:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

cities = ['pune', 'mumbai', 'bangalore', 'delhi', 'hyderabad']
TOTAL_RECORDS = 100000
print(f"Producing {TOTAL_RECORDS} messages to Kafka topic 'retail_orders'...")

for i in range(1, TOTAL_RECORDS + 1):
    order = {
        'order_id': i,
        'amount': random.randint(100, 10000),
        'city': random.choice(cities)   
    }

    producer.send('retail_orders', value=order)
    if i % 1000 == 0:
        print(f"Sent {i} messages so far...")    
        time.sleep(0.01)  # Simulate some delay

producer.flush()
print("Finished producing records.") 