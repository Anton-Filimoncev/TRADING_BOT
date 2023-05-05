import pandas as pd
import numpy as np
from ib_insync import *
# import asyncio
# import scipy.stats as stats
import datetime
import time
from dateutil.relativedelta import relativedelta
import pickle
import pandas_ta as pta

def check_time():
    # чекаем текущее время, если время совпадает со временем работы биржи - время работать
    current_time = datetime.datetime.now().time()
    if current_time < datetime.time(23, 0, 0) and current_time > datetime.time(16, 30, 0):
        return 'work_time'
    else:
        return 'sleep_time'

def nearest_equal(lst, target):
    # ближайшее значение к таргету относительно переданного списка
    return min(lst, key=lambda x: abs(x - target))

def nearest_equal_abs(lst, target):
    return min(lst, key=lambda x: abs(abs(x) - target))

def chain_converter(tickers):

    # разбиваем контракты по грекам и страйкам, для того, что бы удобнее было строить позиции
    # со сложными условиями входа

    iv_bid = []
    iv_ask = []
    delta_bid = []
    delta_ask = []
    gamma_bid = []
    gamma_ask = []
    vega_bid = []
    vega_ask = []
    theta_bid = []
    theta_ask = []
    strike_list = []
    right_list = []
    exp_date_list = []

    for ticker in tickers:
        try:
            iv_bid.append(ticker.bidGreeks.impliedVol)
            iv_ask.append(ticker.askGreeks.impliedVol)
            delta_bid.append(ticker.bidGreeks.delta)
            delta_ask.append(ticker.askGreeks.delta)
            gamma_bid.append(ticker.bidGreeks.gamma)
            gamma_ask.append(ticker.askGreeks.gamma)
            vega_bid.append(ticker.bidGreeks.vega)
            vega_ask.append(ticker.askGreeks.vega)
            theta_bid.append(ticker.bidGreeks.theta)
            theta_ask.append(ticker.askGreeks.theta)
            strike_list.append(ticker.contract.strike)
            right_list.append(ticker.contract.right)
            exp_date_list.append(ticker.contract.lastTradeDateOrContractMonth)

        except:
            pass

    greek_df = pd.DataFrame(
        {
            'IV bid': iv_bid,
            'IV ask': iv_ask,
            'Delta bid': delta_bid,
            'Delta ask': delta_ask,
            'Gamma bid': gamma_bid,
            'Gamma ask': gamma_ask,
            'Vega bid': vega_bid,
            'Vega ask': vega_ask,
            'Theta bid': theta_bid,
            'Theta ask': theta_ask,
            'Strike': strike_list,
            'Right': right_list,
            'EXP_date': exp_date_list,
        }
    )

    df_chains = util.df(tickers)
    df_chains['time'] = df_chains['time'].dt.tz_localize(None)
    df_chains = pd.concat([df_chains, greek_df], axis=1)

    return df_chains

def check_to_open(theoretical_position, strategy, tick):

    # проверяем сложную позицию на наличие такой же сложной позиции в логе открытых позиций
    try:
        OPEN_POSITIONS = pd.read_csv('LOGING_FILES/OPEN_POSITIONS.csv').reset_index(drop=True)

        theoretical_position = theoretical_position.drop_duplicates('contract')
        postion_is_open = False

        for hard_position in OPEN_POSITIONS.hard_position.unique():
            OPEN_POSITIONS_local = OPEN_POSITIONS[OPEN_POSITIONS['hard_position'] == hard_position][::-1].reset_index(drop=True)
            OPEN_POSITIONS_local = OPEN_POSITIONS_local.drop_duplicates('contract')
            if OPEN_POSITIONS_local['contract'].equals(theoretical_position['contract'].astype('str')):
                postion_is_open = True

            # if strategy == 'SPY CALL DEBET SPREAD' or 'SPY PUT CREDIT SPREAD':
            elif tick in OPEN_POSITIONS_local['symbol'].values.tolist():
                postion_is_open = True

    except:
        postion_is_open = False

    print('Postion_is_open: ', postion_is_open)

    return postion_is_open

def get_tech_data(df):
    df['RSI'] = pta.rsi(df['Close'])
    df['SMA_20'] = df['Close'].rolling(window=20).mean()
    df['SMA_100'] = df['Close'].rolling(window=100).mean()

    sma_20 = df['SMA_20'].iloc[-1]
    sma_100 = df['SMA_100'].iloc[-1]
    rsi = df['RSI'].iloc[-1]

    return sma_20, sma_100, rsi

def get_ib_market_price():


