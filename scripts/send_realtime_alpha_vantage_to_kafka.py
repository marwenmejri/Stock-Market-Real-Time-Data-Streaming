import requests
from kafka import KafkaProducer
import json
import time
import logging
import random

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

if producer.bootstrap_connected():
    logging.info("✅ Connected to Kafka brokers.")
else:
    logging.warning("⚠️ Kafka bootstrap connection failed, attempting to continue...")

# --- Symbol to Exchange/Currency mapping
SYMBOL_METADATA = {
    "AAPL": {"exchange": "NASDAQ", "currency": "USD"},
    "MSFT": {"exchange": "NASDAQ", "currency": "USD"},
    "GOOGL": {"exchange": "NASDAQ", "currency": "USD"},
    "TSLA": {"exchange": "NASDAQ", "currency": "USD"},
    "SONY": {"exchange": "TSE", "currency": "JPY"},
    "BABA": {"exchange": "HKEX", "currency": "HKD"}
}

def fetch_intraday_series(symbol, api_key):
    url = (
        f"https://www.alphavantage.co/query?"
        f"function=TIME_SERIES_INTRADAY&symbol={symbol}&interval=1min&apikey={api_key}&outputsize=compact"
    )
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            return data.get("Time Series (1min)", {})
        else:
            logging.warning(f"⚠️ Bad response for {symbol}: {response.status_code}")
    except Exception as e:
        logging.error(f"AlphaVantage request failed for {symbol}: {e}")
    return {}

def push_to_kafka(data, topic_name):
    try:
        result = producer.send(topic_name, value=data)
        metadata = result.get(timeout=10)
        logging.info(f"✅ Sent to {metadata.topic} partition {metadata.partition}, offset {metadata.offset}")
    except Exception as e:
        logging.error(f"Failed to send to Kafka: {e}")

if __name__ == "__main__":
    TOPIC_NAME = "stock-market"
    API_KEY = config.get("ALPHA_VINTAGE_API_KEY")
    symbols = ["AAPL", "MSFT", "GOOGL", "TSLA"]

    for symbol in symbols:
        logging.info(f"📈 Fetching intraday data for {symbol}")
        time_series = fetch_intraday_series(symbol, API_KEY)

        if time_series:
            sorted_timestamps = sorted(time_series.keys())
            for ts in sorted_timestamps:
                data = time_series[ts]
                meta = SYMBOL_METADATA.get(symbol, {"exchange": "UNKNOWN", "currency": "USD"})
                payload = {
                    "symbol": symbol,
                    "timestamp": ts,
                    "open": float(data["1. open"]),
                    "high": float(data["2. high"]),
                    "low": float(data["3. low"]),
                    "close": float(data["4. close"]),
                    "volume": int(data["5. volume"]),
                    "currency": meta["currency"],
                    "exchange": meta["exchange"]
                }
                logging.info(f"Sending to Kafka: {payload}")
                push_to_kafka(payload, TOPIC_NAME)
                # Avoid AlphaVantage rate limits (5 requests/min for free API)
                time.sleep(random.uniform(10, 15))
        else:
            logging.warning(f"No intraday data for {symbol}, skipping...")

    producer.flush()
    producer.close()
    logging.info("✅ Kafka producer closed.")
