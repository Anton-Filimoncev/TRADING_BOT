import pandas as pd
import numpy as np
import datetime as dt
from scipy.signal import argrelextrema
import datetime
import requests
from dateutil.relativedelta import relativedelta
import yfinance as yf
from sklearn import mixture as mix
from scipy import stats

KEY = "ckZsUXdiMTZEZVQ3a25TVEFtMm9SeURsQ1RQdk5yWERHS0RXaWNpWVJ2cz0"

period_set = {}


def true_stage(num):
    return list(period_set.values()).index(num)


def market_stage_vix(df):
    ticker = '^VIX'
    start_vix_price = dt.datetime.now() - relativedelta(years=1)

    # df = yf.download(ticker, start_vix_price, end_vix_price)
    df = df[start_vix_price:]
    # print(df)
    # df = df[['Open', 'High', 'Low', 'Close']]
    df['open'] = df['Open'].shift(1)
    df['high'] = df['High'].shift(1)
    df['low'] = df['Low'].shift(1)
    df['close'] = df['Close'].shift(1)

    df = df[['open', 'high', 'low', 'close']]
    df = df.dropna()

    unsup = mix.GaussianMixture(n_components=4,
                                covariance_type="spherical",
                                n_init=100,
                                random_state=42)
    unsup.fit(np.reshape(df, (-1, df.shape[1])))

    regime = unsup.predict(np.reshape(df, (-1, df.shape[1])))
    df['Return'] = np.log(df['close'] / df['close'].shift(1))
    Regimes = pd.DataFrame(regime, columns=['Regime'], index=df.index) \
        .join(df, how='inner') \
        .assign(market_cu_return=df.Return.cumsum()) \
        .reset_index(drop=False) \
        .rename(columns={'index': 'Date'})

    one_period_price_min = round(Regimes[Regimes['Regime'] == 0]['close'].min(), 2)
    one_period_price_max = round(Regimes[Regimes['Regime'] == 0]['close'].max(), 2)

    two_period_price_min = round(Regimes[Regimes['Regime'] == 1]['close'].min(), 2)
    two_period_price_max = round(Regimes[Regimes['Regime'] == 1]['close'].max(), 2)

    three_period_price_min = round(Regimes[Regimes['Regime'] == 2]['close'].min(), 2)
    three_period_price_max = round(Regimes[Regimes['Regime'] == 2]['close'].max(), 2)

    four_period_price_min = round(Regimes[Regimes['Regime'] == 3]['close'].min(), 2)
    four_period_price_max = round(Regimes[Regimes['Regime'] == 3]['close'].max(), 2)


    one_period_price_mean = Regimes[Regimes['Regime'] == 0]['close'].max()
    two_period_price_mean = Regimes[Regimes['Regime'] == 1]['close'].max()
    three_period_price_mean = Regimes[Regimes['Regime'] == 2]['close'].max()
    four_period_price_mean = Regimes[Regimes['Regime'] == 3]['close'].max()

    lisuss = [one_period_price_mean, two_period_price_mean, three_period_price_mean, four_period_price_mean]
    # print(lisuss)
    lisuss_sort = sorted(lisuss)

    # print(lisuss_sort)
    # print(lisuss.index(four_period_price_max))

    global period_set
    period_set = {0: lisuss_sort.index(lisuss[0])+1,
                  1: lisuss_sort.index(lisuss[1])+1,
                  2: lisuss_sort.index(lisuss[2])+1,
                  3: lisuss_sort.index(lisuss[3])+1,
                  }

    # print(period_set)
    # print(lisuss)
    Regimes['Regime'] = Regimes['Regime'].replace(period_set)
    # Regimes['VIX_Stage'] = Regimes['Regime'].apply(true_stage)
    # print(Regimes[['close', 'Regime', 'VIX_Stage']][:50])
    # print(Regimes['Regime'])
    return Regimes['Regime'].iloc[-1]


# start_date = '2020-01-01'
# market_stage_vix(start_date)
