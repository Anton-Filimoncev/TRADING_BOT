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
from LOGING_FILES.LOGING import logging_open, post_loging_calc
# from VIX_market_stage import market_stage_vix
from VIX_market_stage_2_year import market_stage_vix_2_year
# import pandas_ta as pta


async def vix_short_put(ib, vix_df, input_data):
    strategy = 'VIX SHORT PUT'
    print(f'START {strategy} ~~~~~~~~~~~~~~~~~~~~')
    current_input_data = input_data[input_data['Strategy'] == strategy]

    tick = current_input_data.Stock.values[0]
    atm_short_position = current_input_data.Short.values[0]

    stock_price_df_native = yf.download('^VIX')
    sma_20, sma_100, rsi = get_tech_data(stock_price_df_native)

    # ДАТЫ ЭКСПИРАЦИИ
    limit_date_min = datetime.datetime.now() + relativedelta(days=+25)
    limit_date_max = datetime.datetime.now() + relativedelta(days=+60)

    rights = ['P']

    contract = Stock(tick, 'CBOE', 'USD')
    contract.secType = "IND"

    # print(f'~~~~~ {tick} ~~~~~')

    bars = ib.reqHistoricalData(
        contract, endDateTime='', durationStr='365 D',
        barSizeSetting='1 day', whatToShow='OPTION_IMPLIED_VOLATILITY', useRTH=True)
    #
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

    ticker_contract = Index(conId=ticker_id, symbol=tick, exchange='CBOE', currency='USD', localSymbol=tick)
    # ticker_contract = Index('SPY', 'CBOE')
    ticker_contract.secType = "IND"
    ib.qualifyContracts(ticker_contract)

    ib.reqMarketDataType(1)

    [ticker] = ib.reqTickers(ticker_contract)

    # print('ticker', ticker)

    current_price = ticker.marketPrice()

    vix_signal, vix_percentile = market_stage_vix_2_year(vix_df)

    # print('current_price', current_price)

    # УСЛОВИЯ ВХОДА
    if vix_signal == 1:
        if vix_percentile < 50:
            condition = [f' vix_signal =={vix_signal}, vix_percentile =={vix_percentile}']

            # получаем датасет с ценовыми рядами и греками по опционам
            df_chains = get_df_chains(ticker_contract, limit_date_min, limit_date_max, current_price, tick, rights, ib)

            atm_strike = nearest_equal(df_chains['Strike'].tolist(), current_price)
            atm_put = df_chains[df_chains['Strike'] == atm_strike].reset_index(drop=True).iloc[0]

            contract_to_sell = atm_put['contract']
            contract_to_buy = None

            #  Eсли такая позиция уже открыта True
            if check_to_open(contract_to_buy, contract_to_sell, strategy, tick):
                pass

            else:
                # print(fdasfas)
                # print('-' * 100)
                # print('contract_to_buy', contract_to_buy)
                print('contract_to_sell', contract_to_sell)

                # ---- время закрытия позиции

                exp_exp_date_sell, days_to_exp_sell = get_time_to_exp(contract_to_sell)
                time_to_exp_sell = exp_exp_date_sell - relativedelta(days=int(days_to_exp_sell / 2))

                # чекаем последний айдишник сложных позиций
                try:
                    MAIN_LOG_FILE = pd.read_csv('LOGING_FILES/LOG_FILE.csv').reset_index(drop=True)
                    hard_position = MAIN_LOG_FILE['hard_position'].max() + 1
                except:
                    hard_position = 1

                order_sell = MarketOrder("Sell", atm_short_position)
                trade_sell = ib.placeOrder(contract_to_sell, order_sell)
                while not trade_sell.isDone():
                    ib.waitOnUpdate()
                ib.sleep(15)
                await asyncio.sleep(5)
                # print('shares')
                # print(atm_short_position)

                logg_df = logging_open(trade_sell, contract_to_sell, order_sell, strategy, hard_position, ib,
                                       condition, time_to_exp_sell)
                post_loging_calc()
