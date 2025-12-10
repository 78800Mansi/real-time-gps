# app-code/consumer.py
import os
import json
import psycopg2
from kafka import KafkaConsumer

# Configuration
KAFKA_BROKERS = os.environ.get('KAFKA_BROKERS')
POSTGRES_HOST = os.environ.get('POSTGRES_HOST')
TOPIC_NAME = 'gps-locations'

# Database Setup
def connect_db():
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            database="tracker_db",
            user="gps_user",
            password="supersecurepassword"
        )
        print("Database connection successful.")
        return conn
    except Exception as e:
        print(f"Database connection failed: {e}")
        return None

# Kafka Setup
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_BROKERS.split(','),
    auto_offset_reset='earliest', # Start reading from the beginning if no offset is found
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

if __name__ == "__main__":
    conn = connect_db()
    if conn:
        cursor = conn.cursor()
        
        # Create table if it doesn't exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS location_data (
                id SERIAL PRIMARY KEY,
                truck_id VARCHAR(50),
                latitude NUMERIC,
                longitude NUMERIC,
                timestamp TIMESTAMP
            );
        """)
        conn.commit()

        print(f"Starting GPS Consumer. Reading from {TOPIC_NAME}...")
        
        for message in consumer:
            data = message.value
            
            # 1. Process/Validate Data (e.g., check timestamp)
            
            # 2. Insert into PostgreSQL
            try:
                cursor.execute(
                    """INSERT INTO location_data (truck_id, latitude, longitude, timestamp) 
                       VALUES (%s, %s, %s, to_timestamp(%s))""",
                    (data['truck_id'], data['latitude'], data['longitude'], data['timestamp'])
                )
                conn.commit()
                print(f"Inserted: Truck {data['truck_id']} at {data['timestamp']}")
            except Exception as e:
                print(f"Database insert error: {e}")
                conn.rollback()
    else:
        print("Cannot run consumer without database connection.")