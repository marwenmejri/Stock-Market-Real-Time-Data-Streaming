import io
import yfinance as yf
import pandas as pd
from datetime import datetime
from minio import Minio
from minio.error import S3Error
import json

# Load Minio credentials from config/api_keys.json
with open('config/config.json') as f:
    config = json.load(f)

# Minio Client Setup
minio_client = Minio(
    'localhost:9000',  # Minio server address (mapped from Docker container)
    access_key=config['MINIO']['ACCESS_KEY'],  # MINIO username
    secret_key=config['MINIO']['SECRET_KEY'],  # Minio password
    secure=False  # Disable SSL (set to True if using HTTPS)
)

def fetch_historical_data(symbol, start_date, end_date):
    """
    Fetch historical stock data using yfinance.
    :param symbol: Stock symbol (e.g., "AAPL" for Apple)
    :param start_date: Start date for historical data (format: "YYYY-MM-DD")
    :param end_date: End date for historical data (format: "YYYY-MM-DD")
    :return: Pandas DataFrame containing stock data
    """
    stock_data = yf.download(symbol, start=start_date, end=end_date)

    # Flatten MultiIndex columns
    if isinstance(stock_data.columns, pd.core.indexes.multi.MultiIndex):
        stock_data.columns = ['_'.join(col).strip() for col in stock_data.columns.values]

    # Reset the index to move 'Date' into a column
    stock_data = stock_data.reset_index()

    return stock_data

def save_to_minio(data, bucket_name):
    """
    Save stock data to Minio as a CSV file.
    :param data: Pandas DataFrame containing stock data
    :param bucket_name: Name of the Minio bucket
    """
    csv_data = data.to_csv(index=True).encode('utf-8')  # Convert DataFrame to CSV and encode as bytes
    csv_file = io.BytesIO(csv_data)  # Wrap the bytes in a file-like object

    try:
        minio_client.put_object(
            bucket_name=bucket_name,
            object_name=f"raw/{datetime.now().strftime('%Y-%m-%d')}_yahoo_stock_data.csv",  # File name with timestamp
            data=csv_file,  # Pass the file-like object
            length=len(csv_data),  # Specify the length of the data
            content_type='application/csv'
        )
        print("Data successfully pushed to Minio.")
    except S3Error as e:
        print(f"Error uploading to Minio: {e}")

if __name__ == "__main__":
    SYMBOL = "AAPL"  # Example stock symbol
    START_DATE = "2020-01-01"
    END_DATE = "2023-01-01"
    BUCKET_NAME = "raw-data"

    # Fetch historical data
    historical_data = fetch_historical_data(SYMBOL, START_DATE, END_DATE)

    # Save data to Minio
    save_to_minio(historical_data, BUCKET_NAME)