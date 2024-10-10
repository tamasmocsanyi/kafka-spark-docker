from kafka import KafkaConsumer

# Kafka consumer configuration
consumer = KafkaConsumer(
    'coordinates',  # The Kafka topic to subscribe to
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',  # Start from the earliest message if not committed
    enable_auto_commit=True,
    group_id='coordinates-group'  # Consumer group ID
)

# Listen for messages from Kafka
print("Listening to Kafka topic 'coordinates'...")
for message in consumer:
    print(f"Received message: {message.value.decode('utf-8')}")