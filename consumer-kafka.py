import signal
import sys
from kafka import KafkaConsumer
import json
import sqlite3
import time
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Delete the existing database if it exists
if os.path.exists('Kafka.db'):
    os.remove('Kafka.db')

# Function to create a Kafka consumer
def create_consumer():
    try:
        consumer = KafkaConsumer(
            'reate_hist_topic', #The name of the Kafka topic to subscribe to
            bootstrap_servers='localhost:9092',
            auto_offset_reset='earliest', #means it will start from the beginning of the topic as current offset does not exists
            enable_auto_commit=True,
            group_id='my-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        logging.info("Kafka consumer created successfully.")
        return consumer
    except Exception as e:
        logging.error(f'Error initializing Kafka consumer: {e}')
        return None

# Function to create a SQLite database connection and table
def create_db():
    try:
        conn = sqlite3.connect('Kafka.db')
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS funding_rate_hist (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fundingRate REAL,
                fundingTime REAL,
                instId TEXT,
                instType TEXT,
                method TEXT,
                realizedRate REAL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()
        logging.info("SQLite database and table created successfully.")
        return conn
    except sqlite3.Error as e:
        logging.error(f'SQLite error: {e}')
        return None

# Function to insert data into the SQLite table
def insert_data(conn, data):
    try:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO funding_rate_hist (
               fundingRate,fundingTime,instId,instType,method,realizedRate,timestamp
            ) VALUES (?,?,?,?,?,?,CURRENT_TIMESTAMP)
        ''', (
            data['fundingRate'], data['fundingTime'], data['instId'], data['instType'], data['method'], data['realizedRate']
        ))
        conn.commit()
        logging.info(f"Data inserted: {data}")
    except sqlite3.DatabaseError as db_err:
        logging.error(f"Database error: {db_err}")
        conn.rollback()
    except Exception as e:
        logging.error(f"Error inserting data: {e}")


def consume_messages(consumer, conn):
    try:
        for message in consumer:
            logging.info(f'Received: {message.value}')
            insert_data(conn, message.value)
    except Exception as e:
        logging.error(f'Error occurred: {e}')
        logging.info('Closing consumer and reconnecting to Kafka...')
        consumer.close()
        time.sleep(5)  # Wait for 5 seconds before reconnecting
        consumer = create_consumer()


def signal_handler(sig, frame):
    logging.info('Shutting down gracefully...')
    if consumer:
        consumer.close()
    if conn:
        conn.close()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

if __name__ == "__main__":
    consumer = create_consumer()
    conn = create_db()

    if consumer and conn:
        consume_messages(consumer, conn)
    else:
        logging.error('Failed to create Kafka consumer or database connection.')
