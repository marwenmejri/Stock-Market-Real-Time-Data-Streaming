import requests
from kafka import KafkaProducer
import json
import time
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load config
with open('config/config.json') as f:
    config = json.load(f)

with open('config/kafka_config.json') as f:
    kafka_config = json.load(f)

# Kafka Producer Setup
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    acks="all",
    retries=5,
    linger_ms=10,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Test connection
if producer.bootstrap_connected():
    logging.info("Connected to Kafka brokers.")
else:
    logging.warning("Kafka bootstrap connection failed. Continuing anyway...")

def fetch_intraday_series(symbol, api_key):
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval=1min&apikey={api_key}&outputsize=compact"
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            return data.get("Time Series (1min)", {})
    except Exception as e:
        logging.error(f"Alpha Vantage request failed: {e}")
    return {}

def push_to_kafka(data, topic_name):
    try:
        result = producer.send(topic_name, value=data)
        metadata = result.get(timeout=10)
        logging.info(f"Sent to topic {metadata.topic} partition {metadata.partition}, offset {metadata.offset}")
    except Exception as e:
        logging.error(f"Failed to send to Kafka: {e}")

if __name__ == "__main__":
    SYMBOL = "AAPL"
    TOPIC_NAME = "stock-market"
    API_KEY = config['ALPHA_VINTAGE_API_KEY']

    try:
        time_series = fetch_intraday_series(SYMBOL, API_KEY)
        if time_series:
            sorted_timestamps = sorted(time_series.keys())
            for ts in sorted_timestamps:
                data = time_series[ts]
                payload = {
                    "symbol": SYMBOL,
                    "timestamp": ts,
                    "open": float(data["1. open"]),
                    "high": float(data["2. high"]),
                    "low": float(data["3. low"]),
                    "close": float(data["4. close"]),
                    "volume": int(data["5. volume"])
                }
                logging.info(f"Sending: {payload}")
                push_to_kafka(payload, TOPIC_NAME)
                time.sleep(2)
        else:
            logging.warning("No intraday time series data returned.")
    except Exception as e:
        logging.error(f"Script failed: {e}")
    finally:
        producer.flush()
        producer.close()
        logging.info("Kafka producer closed.")
