# app-code/producer.py
import os
import json
import time
import uuid
from kafka import KafkaProducer
from random import uniform

# Configuration (from Kubernetes environment variables)
KAFKA_BROKERS = os.environ.get('KAFKA_BROKERS')
TOPIC_NAME = 'gps-locations'

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKERS.split(','),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_location_data():
    """Generates a mock GPS event for a truck."""
    return {
        'truck_id': f"TRUCK-{uuid.uuid4().hex[:6]}",
        'latitude': uniform(34.0, 34.5),  # Mock coordinates near a city
        'longitude': uniform(-118.0, -118.5),
        'timestamp': time.time()
    }

if __name__ == "__main__":
    print(f"Starting GPS Producer. Sending data to {TOPIC_NAME}...")
    while True:
        data = generate_location_data()
        future = producer.send(TOPIC_NAME, value=data)
        
        # Optional: Wait for acknowledgment to ensure delivery
        try:
            record_metadata = future.get(timeout=10)
            print(f"Sent: {data['truck_id']} @ {data['latitude']:.4f}")
        except Exception as e:
            print(f"Error sending message: {e}")
        
        time.sleep(1) # Send a new location every 1 second