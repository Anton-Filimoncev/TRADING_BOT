# import pandas as pd
# import numpy as np
# from ib_insync import *
import asyncio
import scipy.stats as stats
# import datetime
# import time
# from dateutil.relativedelta import relativedelta
import yfinance as yf
# import pickle
from support import *
from LOGING_FILES.LOGING import logging_open
from VIX_market_stage import market_stage_vix
# import pandas_ta as pta


async def credit_straddle_strat(ib, vix_df, input_data, yahoo_stock):
    strategy = 'CREDIT STRADDLE'
    print(f'START {strategy} ~~~~~~~~~~~~~~~~~~~~')
    current_input_data = input_data[input_data['Strategy'] == strategy]

    tick = current_input_data.Stock.values[0]
    atm_call_position = current_input_data.Short.values[0]
    atm_put_possition = current_input_data.Short.values[0]

    # ДАТЫ ЭКСПИРАЦИИ
    limit_date_min = datetime.datetime.now() + relativedelta(days=+30)
    limit_date_max = datetime.datetime.now() + relativedelta(days=+90)

    rights = ['C', 'P']

    contract = Stock(tick, 'SMART', 'USD')

    # print(f'------- {tick} --------')

    bars = ib.reqHistoricalData(
        contract, endDateTime='', durationStr='365 D',
        barSizeSetting='1 day', whatToShow='OPTION_IMPLIED_VOLATILITY', useRTH=True)

    # print(bars)
    df_iv = util.df(bars)

    # print(df_iv.columns.tolist())
    df_iv['IV_percentile'] = df_iv['close'].rolling(364).apply(
        lambda x: stats.percentileofscore(x, x.iloc[-1]))

    # print('IV_percentile', df_iv['IV_percentile'].iloc[-1])

    # df_iv['IV_percentile'].iloc[-1] = 90

    # получение айдишника тикера
    bars_id = ib.qualifyContracts(contract)

    df_bars_id = util.df(bars_id)
    ticker_id = df_bars_id['conId'].values[0]
    # print(df_bars_id)
    # print(ticker_id)

    ticker_contract = Index(conId=ticker_id, symbol=tick, exchange='SMART', currency='USD', localSymbol=tick)
    # ticker_contract = Index('SPY', 'CBOE')
    ib.qualifyContracts(ticker_contract)

    ib.reqMarketDataType(1)
    # 1 = Live
    # 2 = Frozen
    # 3 = Delayed
    # 4 = Delayed frozen

    [ticker] = ib.reqTickers(ticker_contract)

    # print('ticker', ticker)

    current_price = ticker.marketPrice()

    stock_price_df_native = yahoo_stock[tick]
    sma_20, sma_100, rsi = get_tech_data(stock_price_df_native)
    # print('current_price', current_price)


    # УСЛОВИЯ ВХОДА
    if current_price > sma_100 and current_price < sma_20 and 30 < rsi < 70 or current_price < sma_100 and current_price > sma_20 and 30 < rsi < 70:
        if df_iv['IV_percentile'].iloc[-1] > 50:
            vix_signal = market_stage_vix(vix_df)

            condition = [f'IV_percentile {df_iv["IV_percentile"].iloc[-1]} < 50, vix_signal =={vix_signal}'
                         f'RSI: {rsi}, price: {current_price} > sma_100: {sma_100},  sma_20: {sma_20} > sma_100: {sma_100}']

            # получаем датасет с ценовыми рядами и греками по опционам
            df_chains = get_df_chains(ticker_contract, limit_date_min, limit_date_max, current_price, tick,
                                      rights, ib)

            # фильтруем нужные контракту по страйку условия
            atm_strike = nearest_equal(df_chains['Strike'].tolist(), current_price)
            atm_call = df_chains[df_chains['Strike'] == atm_strike].reset_index(drop=True).iloc[0]
            atm_call = atm_call[atm_call['Right'] == 'C']

            atm_put = df_chains[df_chains['Strike'] == atm_strike].reset_index(drop=True).iloc[0]
            atm_put = atm_put[atm_put['Right'] == 'P']

            contract_to_sell_put = atm_put['contract']
            contract_to_sell_call = atm_call['contract']

            #  Eсли такая позиция уже открыта True
            if check_to_open(contract_to_sell_put, contract_to_sell_call, strategy, tick):
                pass

            else:
                # print(fdasfas)
                # print('-' * 100)
                print('contract_to_sell_put', contract_to_sell_put)
                print('contract_to_sell_call', contract_to_sell_call)

                # ---- время закрытия позиции

                exp_exp_date_put, days_to_exp_put = get_time_to_exp(contract_to_sell_put)
                time_to_exp_put = exp_exp_date_put - relativedelta(days=int(days_to_exp_put / 3))

                exp_exp_date_call, days_to_exp_call = get_time_to_exp(contract_to_sell_call)
                time_to_exp_call = exp_exp_date_call - relativedelta(days=1)

                # чекаем последний айдишник сложных позиций
                try:
                    MAIN_LOG_FILE = pd.read_csv('LOGING_FILES/LOG_FILE.csv').reset_index(drop=True)
                    hard_position = MAIN_LOG_FILE['hard_position'].max() + 1
                except:
                    hard_position = 1

                order_put = MarketOrder("Sell", atm_put_possition)
                trade_put = ib.placeOrder(contract_to_sell_put, order_put)
                while not trade_put.isDone():
                    ib.waitOnUpdate()
                ib.sleep(5)
                await asyncio.sleep(5)
                logg_df = logging_open(trade_put, contract_to_sell_put, order_put, strategy, hard_position, ib,
                                       condition, time_to_exp_put)

                order_call = MarketOrder("Sell", atm_call_position)
                trade_call = ib.placeOrder(contract_to_sell_call, order_call)
                while not trade_call.isDone():
                    ib.waitOnUpdate()
                ib.sleep(15)
                await asyncio.sleep(5)
                # print('shares')
                # print(atm_call_position)
                # print(atm_put_possition)

                logg_df = logging_open(trade_call, contract_to_sell_call, order_call, strategy, hard_position,
                                       ib, condition, time_to_exp_call)
                # print('end while')


