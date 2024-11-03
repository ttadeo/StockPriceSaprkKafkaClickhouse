from kafka import KafkaProducer
import yfinance as yf
import json
import time

# Create a Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Fetch stock data
def fetch_stock_data(stock_symbol):
    stock = yf.Ticker(stock_symbol)
    # Fetch historical stock data as a pandas DataFrame
    hist = stock.history(period="1d", interval="1m")
    # Reset the index to access timestamp as a column and convert timestamps to strings
    hist.reset_index(inplace=True)
    hist['Datetime'] = hist['Datetime'].astype(str)  # Convert Timestamp to string
    hist['symbol'] = stock_symbol  # Add the stock symbol to each record
    # Convert the DataFrame to a dictionary
    stock_dict = hist.to_dict(orient='records')
    return stock_dict

# Send stock data to Kafka
while True:
    stock_symbol = 'AAPL'  # Replace with any stock symbol
    stock_data = fetch_stock_data(stock_symbol)
    
    for record in stock_data:
        # Send each record individually to Kafka
        producer.send('stock_prices', value=record)
        print(f"Sent record: {record}")
    
    time.sleep(60)  # Fetch and send data every minute
