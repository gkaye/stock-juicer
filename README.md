# Stock Juicer

Stock Juicer is a tool for screening stocks in real-time based on specific criteria such as linearity, liquidity, volume, and Average True Range (ATR). It uses the Alpaca API and the Dash framework to visualize and analyze stock market data.

## Features

- **Stock Market Screening**: The screener filters and sorts stocks using defined criteria like linearity, liquidity, volume, and ATR.
- **Real-Time Intraday Charts**: Monitor live charts based on the output of the screener, updated in real time.
- **Dynamic Chart Display**: Charts can be pinned to float on top for easy viewing as the order updates.
- **Performant Data Processing**: This application leverages Python multithreading and the Dash framework to process and display large datasets, efficiently rendering OHLC candlestick data from raw trade data.

## Requirements

- Python 3.x
- alpaca_trade_api
- dash
- pandas

## Setup

Set up your Alpaca API credentials and insert them into the <API_KEY> and <SECRET_KEY> placeholders in live_chart.py.


## Run the Application

To start the application and begin viewing real-time stock market data, run the following command:

```bash
python live_chart.py
```

## Screenshots
![stock juicer screenshot](https://github.com/user-attachments/assets/2d6e4b48-46c0-4ff6-9bb3-2437c52abf30)
