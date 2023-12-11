import os
import requests   
import pandas as pd

import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.util import symbol
from datetime import datetime

from dotenv import load_dotenv

usr = os.environ.get('USR')
pw = os.environ.get('PW')

alchemyEngine   = create_engine(f'postgresql+psycopg2://{usr}:{pw}@127.0.0.1/marketdata');
dbConnection    = alchemyEngine.connect();






# get klines from binance

def binance_klines(symbol,s: datetime, l:int):
    url = f'https://api.binance.com/api/v3/klines?symbol={symbol}USDT&interval=1h&limit=1000'
    r = requests.get(url)
    data = r.json()
    return data


def main():
    symbol = 'BTC'
    l = 1000
    data_raw = binance_klines(symbol, s,l)

    # convert to pandas dataframe


    cols = ['ts', 'open', 'high', 'low', 'close', 'volume', 'ts_close', 'qav', 'num_trades', 'taker_base_vol', 'taker_quote_vol', 'ignore']
    df_parsed = pd.DataFrame(data_raw,columns=cols)
    df = df_parsed[['ts', 'open', 'high', 'low', 'close', 'volume']]
    df['sym'] = [symbol] * len(df)
    df.rename(columns={
        'open':'o',
        'high':'h',
        'low':'l',
        'close':'c',
        'volume':'v',
    }, inplace=True)
    print(df.head())
    print(df.info())

if __name__ == '__main__':
    main()

"""
[
  [
    1499040000000,      // Kline open time
    "0.01634790",       // Open price
    "0.80000000",       // High price
    "0.01575800",       // Low price
    "0.01577100",       // Close price
    "148976.11427815",  // Volume
    1499644799999,      // Kline Close time
    "2434.19055334",    // Quote asset volume
    308,                // Number of trades
    "1756.87402397",    // Taker buy base asset volume
    "28.46694368",      // Taker buy quote asset volume
    "0"                 // Unused field, ignore.
  ]
]

"""