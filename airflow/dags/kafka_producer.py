import json
import time
from kafka import KafkaProducer

# Kafka producer configuration
kafka_conf = {
    'bootstrap_servers': 'localhost:9092', 
    'client_id': 'coordinates'
}

producer = KafkaProducer(**kafka_conf)  # Unpack dictionary into kwargs

# Function to read coordinates from JSON file
def read_coordinates_from_json(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
        return data['features'][0]['geometry']['coordinates']

# Send messages to Kafka with delay
def send_coordinates_to_kafka(topic, coordinates):
    for coordinate in coordinates:
        message = json.dumps({'longitude': coordinate[0], 'latitude': coordinate[1]})  # Exclude altitude
        try:
            producer.send(topic, value=message.encode('utf-8'))  # Ensure message is encoded
            print(f'Sent: {message}')
            time.sleep(5)  # Simulate real-time data
        except Exception as e:
            print(f"Error sending message: {e}")

if __name__ == "__main__":
    file_path = 'C:/Users/tommox/Desktop/GRADED_ASSESMENT/kafka-docker/GPS_PROJECT/coordinates.json'  
    kafka_topic = 'coordinates'

    coordinates = read_coordinates_from_json(file_path)
    send_coordinates_to_kafka(kafka_topic, coordinates) 