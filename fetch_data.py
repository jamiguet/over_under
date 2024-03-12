import ccxt
import toml
import datetime
import time
import pandas as pd
import requests
import sqlalchemy as sq

# TODO Convert to parametrised script with following inputs:
#  * Exchange
#  * Start time
#  * End time
#  * Market filtering criteria i.e. 3S, 3L ...


def get_my_ip():
    response = requests.get('https://api.ipify.org')
    print('My public IP address is:', response.text)


def init():
    config = toml.load('.streamlit/secrets.toml')
    exchange = ccxt.kucoin({
        'adjustForTimeDifference': True,
        "apiKey": config['KUCOIN_API_KEY'],
        "secret": config['KUCOIN_API_SECRET'],
        'password': config['PASSWORD'],
    })

    exchange.load_markets()
    conn_params = config['connections']['ohlc']
    db = sq.create_engine(
        f"{conn_params['dialect']}://{conn_params['username']}:{conn_params['password']}@"
        f"{conn_params['host']}:{conn_params['port']}/{conn_params['database']}?sslmode=require")
    db = db.connect()
    return exchange, db


if __name__ == '__main__':
    k_con, db = init()
    get_my_ip()
    since = int(time.mktime((datetime.datetime.now() - datetime.timedelta(days=0)).timetuple())) * 1000
    market_list = list(filter(lambda x: "/USDT" in x, k_con.symbols))
    print(f"Available markets# {len(market_list)}")

    for idx, symbol in enumerate(market_list):
        print(f"Fetching {symbol} ({idx}/{len(market_list)})")
        data = k_con.fetch_ohlcv(symbol, timeframe='1d', since=since, limit=None)
        data_df = pd.DataFrame(data=data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        data_df['timestamp'] = pd.to_datetime(data_df['timestamp'], unit='ms')
        data_df['symbol'] = symbol
        data_df.to_sql('candlesticks', db, schema='ohlc', index=False, if_exists='append')
