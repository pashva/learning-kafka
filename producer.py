from kafka import KafkaProducer
import time
# Initialize the Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # Kafka broker address
    key_serializer=lambda k: k.encode('utf-8'),  # Key serializer
    value_serializer=lambda v: v.encode('utf-8')  # Value serializer
)

# Topic name
topic = 'source_codes'

# Send some messages to the Kafka topic
try:
    for i in range(10):
        key = f'key-{i}'
        value = f'value-{i}'
        if i%2 == 0:
            future = producer.send(topic, key=key, value=value, partition=0)
        else:
            future = producer.send(topic, key=key, value=value, partition=1)
        
        # Block for 'synchronous' sends
        result = future.get(timeout=10)
        print(f"Sent {key}: {value} to partition {result.partition}")
        time.sleep(2)

finally:
    # Clean up and close the producer
    producer.close()

print('Messages have been sent!')
