import websocket
import json
import logging
import time
from kafka import KafkaProducer
from collections import deque
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load configs
with open("config/kafka_config.json") as f:
    kafka_config = json.load(f)

with open("config/config.json") as f:
    config = json.load(f)

# Kafka setup
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Check connection
if producer.bootstrap_connected():
    logging.info("Connected to Kafka brokers inside Docker network.")
else:
    logging.warning("Kafka bootstrap connection failed inside container.")

# Keep a sliding window of the last 12 prices (~1 min)
price_window = deque(maxlen=12)

# WebSocket callbacks
def on_message(ws, message):
    try:
        parsed = json.loads(message)

        if parsed.get("event") == "price":
            price = parsed.get("price")
            ts_epoch = parsed.get("timestamp")
            symbol = parsed.get("symbol")

            if price and ts_epoch:
                price_window.append({
                    "price": price,
                    "timestamp": ts_epoch
                })

                logging.info(f"Received tick: {price} at {ts_epoch}")

                # Emit synthetic OHLC every 12 ticks
                if len(price_window) == 12:
                    prices = [p["price"] for p in price_window]
                    ts_iso = datetime.utcfromtimestamp(ts_epoch).isoformat()

                    candle = {
                        "symbol": symbol,
                        "timestamp": ts_iso,
                        "open": prices[0],
                        "high": max(prices),
                        "low": min(prices),
                        "close": prices[-1],
                        "volume": 0  # Not available via WebSocket
                    }

                    result = producer.send("stock-market", value=candle)
                    metadata = result.get(timeout=10)
                    logging.info(f"Emitted candle: {candle} -> topic {metadata.topic}, offset {metadata.offset}")

                    price_window.clear()

    except Exception as e:
        logging.error(f"WebSocket message processing error: {e}")

def on_error(ws, error):
    logging.error(f"WebSocket error: {error}")

def on_close(ws, close_status_code, close_msg):
    logging.warning("WebSocket closed.")

def on_open(ws):
    logging.info("WebSocket connection opened.")
    ws.send(json.dumps({
        "action": "subscribe",
        "params": {
            "symbols": "AAPL"
        }
    }))

# Entrypoint
if __name__ == "__main__":
    try:
        url = f"wss://ws.twelvedata.com/v1/quotes/price?apikey={config['TWELVE_DATA_API_KEY']}"
        ws = websocket.WebSocketApp(
            url,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close
        )
        ws.run_forever()
    except Exception as e:
        logging.error(f"Fatal error in main: {e}")
    finally:
        logging.info("Tearing down...")
        producer.flush()
        producer.close()
        logging.info("Kafka producer closed.")
