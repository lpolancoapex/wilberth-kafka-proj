import signal
import sys
from kafka import KafkaProducer
import requests
import json
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_okx_data(retries=3, delay=5):
    url = f'https://www.okx.com/api/v5/public/funding-rate-history?instId=BTC-USD-SWAP'
    for attempt in range(retries):
        try:
            response = requests.get(url, verify=False)  # Disable SSL verification
            response.raise_for_status()  # Raise an HTTPError for bad responses
            data = response.json()
            return data['data']
        except requests.exceptions.HTTPError as http_err:
            logger.error(f'HTTP error occurred: {http_err}')
        except requests.exceptions.RequestException as req_err:
            logger.error(f'Request error occurred: {req_err}')
        except json.JSONDecodeError as json_err:
            logger.error(f'JSON decode error occurred: {json_err}')
        
        if attempt < retries - 1:
            logger.info(f'Retrying in {delay} seconds...')
            time.sleep(delay)
    return None

def clean_data(data):
    cleaned_data = []
    for item in data:
        cleaned_item = {
            'fundingRate': float(item.get('fundingRate', 0) or 0),
            'fundingTime': float(item.get('fundingTime', 0) or 0),
            'instId':item.get('instId',''),
            'instType':item.get('instType',''),
            'method':item.get('method','').upper(),
            'realizedRate': float(item.get('realizedRate', 0) or 0)
        }
        cleaned_data.append(cleaned_item)
    return cleaned_data

def transformations(data):
    for item in data:
        item['rate_per_time'] = item['fundingRate'] / item['fundingTime'] if item['fundingTime'] != 0 else 0
    return data
    return data

# Send messages to the Kafka topic with retry mechanism
def send_data(retries=3, delay=5):
    while True: #To ensure data is fetched continuously in case of failure and after max retries attempt
        data = fetch_okx_data()
        if data:
            cleaned_data = clean_data(data)
            transformed_data = transformations(cleaned_data)
            for item in transformed_data:
                success = False
                for attempt in range(retries):
                    try:
                        producer.send('reate_hist_topic', value=item) #Send message to okx_topic
                        logger.info(f'Sent: {item}')    #Sent message that indicates item was successfully send to topic 
                        success = True #Set success flag to true
                        break  # Break the retry loop if send is successful 
                    except Exception as e:
                        logger.error(f'Error sending data to Kafka: {e}')
                        if attempt < retries - 1:
                            logger.info(f'Retrying in {delay} seconds...')
                            time.sleep(delay)
                if not success:
                    logger.error(f'Failed to send data after {retries} attempts: {item}')
        else:
            logger.info('No data fetched from OKX API.')
        time.sleep(10)  # Fetch data every 60 seconds


def signal_handler(sig, frame):
    logger.info('Shutting down gracefully...')
    producer.close()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

if __name__ == "__main__":
    send_data()
