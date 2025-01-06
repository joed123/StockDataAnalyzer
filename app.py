import requests
import json
import pandas as pd
import pandas_ta as ta


api_key = ''
symbol = 'AAPL'
base_url = 'https://www.alphavantage.co/query?'
query = f'function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}'

url = base_url + query
response = requests.get(url)
data = response.json()
print(data)

time_series = data['Time Series (Daily)']


with open('time_series.json', 'w') as f:
    json.dump(time_series, f)


def process_data(time_series):
    df = pd.DataFrame.from_dict(time_series, orient='index')

    df.columns = ['open', 'high', 'low', 'close', 'volume']
    df = df.astype({
        'open': 'float',
        'high': 'float',
        'low': 'float',
        'close': 'float',
        'volume': 'int'
    })

    print("Cleaned DataFrame:")
    print(df)

    return df


def add_technical_indicators(df):
    df['RSI'] = ta.rsi(df['close'], length=14)

    bbands = ta.bbands(df['close'], length=20, std=2)
    df = df.join(bbands)

    macd = ta.macd(df['close'], fast=12, slow=26, signal=9)
    df = df.join(macd)

    return df


cleaned_df = process_data(time_series)
df_with_indicators = add_technical_indicators(cleaned_df)


def process_stock_data():
    df = pd.DataFrame.from_dict(time_series, orient='index')
    df.columns = ['open', 'high', 'low', 'close', 'volume']
    df = df.astype({'open': 'float', 'high': 'float', 'low': 'float', 'close': 
                    'float', 'volume': 'int'})

    df['RSI'] = ta.rsi(df['close'], length=14)
    df = df.join(ta.bbands(df['close'], length=20, std=2))
    df = df.join(ta.macd(df['close'], fast=12, slow=26, signal=9))

    return df

