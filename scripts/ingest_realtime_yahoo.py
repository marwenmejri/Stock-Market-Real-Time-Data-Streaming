import yfinance as yf
from kafka import KafkaProducer
import json
import time
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load Kafka configuration
with open('config/kafka_config.json') as f:
    kafka_config = json.load(f)

# Kafka Producer Setup
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Check connection
if producer.bootstrap_connected():
    logging.info("Connected to Kafka broker(s).")
else:
    logging.error("Failed to connect to Kafka broker(s).")

def fetch_intraday_data(symbol):
    stock = yf.Ticker(symbol)
    hist = stock.history(interval="1m", period="1d")
    return hist if not hist.empty else None

def push_to_kafka(data, topic_name):
    try:
        result = producer.send(topic_name, value=data)
        record_metadata = result.get(timeout=10)
        logging.info(f"Message sent to topic {record_metadata.topic}, partition {record_metadata.partition}, offset {record_metadata.offset}")
    except Exception as e:
        logging.error(f"Failed to send message: {e}")

if __name__ == "__main__":
    SYMBOL = "AAPL"
    TOPIC_NAME = "stock-market"

    try:
        hist_data = fetch_intraday_data(SYMBOL)
        if hist_data is not None:
            for index, row in hist_data.iterrows():
                payload = {
                    "symbol": SYMBOL,
                    "timestamp": str(index),
                    "open": row["Open"],
                    "high": row["High"],
                    "low": row["Low"],
                    "close": row["Close"],
                    "volume": row["Volume"]
                }
                logging.info(f"Sending: {payload}")
                push_to_kafka(payload, TOPIC_NAME)
                time.sleep(5)
        else:
            logging.warning("No historical intraday data available.")
    except Exception as e:
        logging.error(f"Error during fetch/publish: {e}")
    finally:
        producer.flush()
        producer.close()
        logging.info("Kafka producer closed.")
