import requests
import json
import pandas as pd
from datetime import datetime
from minio import Minio
from minio.error import S3Error
import io

# Load API keys and Minio credentials from config/api_keys.json
with open('config/config.json') as f:
    config = json.load(f)

# Minio Client Setup
minio_client = Minio(
    'minio:9000',  # Minio server address (mapped from Docker container)
    access_key=config['MINIO']['ACCESS_KEY'],  # MINIO username
    secret_key=config['MINIO']['SECRET_KEY'],  # Minio password
    secure=False  # Disable SSL (set to True if using HTTPS)
)


def fetch_historical_data(symbol, api_key):
    """
    Fetch historical stock data from Alpha Vantage.
    :param symbol: Stock symbol (e.g., "AAPL" for Apple)
    :param api_key: Alpha Vantage API key
    :return: Pandas DataFrame containing stock data
    """
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&interval=1min&apikey={api_key}&outputsize=full"
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        with open("alphavantage_historical_data.json", "w") as f:
            json.dump(data, f, indent=4)
        
        # Extract the time series data
        time_series = data.get("Time Series (Daily)", {})
        
        # Parse the data into a list of records
        records = []
        for date, values in time_series.items():
            record = {
                "date": date,
                "open": float(values["1. open"]),
                "high": float(values["2. high"]),
                "low": float(values["3. low"]),
                "close": float(values["4. close"]),
                "volume": int(values["5. volume"])
            }
            records.append(record)
        
        # Convert to DataFrame
        df = pd.DataFrame(records)
        return df
    else:
        raise Exception(f"Failed to fetch data: {response.status_code}")

def save_to_minio(data, bucket_name):
    """
    Save stock data to Minio as a CSV file.
    :param data: Pandas DataFrame containing stock data
    :param bucket_name: Name of the Minio bucket
    """
    csv_data = data.to_csv(index=False).encode('utf-8')  # Convert DataFrame to CSV and encode as bytes
    csv_file = io.BytesIO(csv_data)  # Wrap the bytes in a file-like object

    try:
        minio_client.put_object(
            bucket_name=bucket_name,
            object_name=f"raw/{datetime.now().strftime('%Y-%m-%d')}_alpha_stock_data.csv",  # File name with timestamp
            data=csv_file,  # Pass the file-like object
            length=len(csv_data),  # Specify the length of the data
            content_type='application/csv'
        )
        print("Data successfully pushed to Minio.")
    except S3Error as e:
        print(f"Error uploading to Minio: {e}")

if __name__ == "__main__":
    SYMBOL = "AAPL"  # Example stock symbol
    BUCKET_NAME = "raw-data"
    # Alpha Vantage API Key
    API_KEY = config['ALPHA_VINTAGE_API_KEY']

    # Fetch historical data
    historical_data = fetch_historical_data(SYMBOL, API_KEY)

    # Save data to Minio
    save_to_minio(historical_data, BUCKET_NAME)