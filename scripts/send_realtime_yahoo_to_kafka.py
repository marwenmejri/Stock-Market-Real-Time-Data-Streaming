import yfinance as yf
from kafka import KafkaProducer
import json
import time
import logging
import pandas as pd

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Load Kafka configuration
with open("config/kafka_config.json") as f:
    kafka_config = json.load(f)

# Kafka Producer Setup
producer = KafkaProducer(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    acks="all",
    retries=5,
    linger_ms=10,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# Check connection
if producer.bootstrap_connected():
    logging.info("Connected to Kafka broker(s).")
else:
    logging.error("Failed to connect to Kafka broker(s).")


def fetch_intraday_batch(symbols):
    """Download batched 5-minute data for multiple tickers (less rate limiting)."""
    data = yf.download(
        " ".join(symbols),
        period="5d",
        interval="5m",
        group_by="ticker",
        auto_adjust=False,
        threads=True,
    )
    return data if not data.empty else None


def push_to_kafka(data, topic_name):
    try:
        result = producer.send(topic_name, value=data)
        record_metadata = result.get(timeout=10)
        logging.info(
            f"Message sent to topic {record_metadata.topic}, "
            f"partition {record_metadata.partition}, offset {record_metadata.offset}"
        )
    except Exception as e:
        logging.error(f"Failed to send message: {e}")


if __name__ == "__main__":
    SYMBOLS = ["AAPL", "MSFT", "GOOGL"]
    TOPIC_NAME = "stock-market"

    try:
        hist_data = fetch_intraday_batch(SYMBOLS)
        if hist_data is not None:
            for symbol in SYMBOLS:
                # Access symbol-specific data
                sym_data = hist_data[symbol] if isinstance(hist_data.columns, pd.MultiIndex) else hist_data
                for index, row in sym_data.iterrows():
                    payload = {
                        "symbol": symbol,
                        "timestamp": str(index),
                        "open": row["Open"],
                        "high": row["High"],
                        "low": row["Low"],
                        "close": row["Close"],
                        "volume": int(row["Volume"]),
                        "currency": "USD",
                        "exchange": "NASDAQ",
                    }
                    logging.info(f"Sending: {payload}")
                    push_to_kafka(payload, TOPIC_NAME)
                    time.sleep(2)  # simulate realtime flow
        else:
            logging.warning("No historical intraday data available.")
    except Exception as e:
        logging.error(f"Error during fetch/publish: {e}")
    finally:
        producer.flush()
        producer.close()
        logging.info("Kafka producer closed.")
