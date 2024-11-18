from datetime import datetime

import yfinance as yf

ticker = yf.Ticker('AAPL')
print(ticker.info)

