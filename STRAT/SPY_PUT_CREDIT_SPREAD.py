import pandas as pd
import numpy as np
from ib_insync import *
import asyncio
import scipy.stats as stats
import datetime
import time
from dateutil.relativedelta import relativedelta
import yfinance as yf
import pickle
from support import *
from LOGING_FILES.LOGING import logging_open, post_loging_calc
from VIX_market_stage import market_stage_vix
import pandas_ta as pta


async def spy_put_credit_spread_strat(ib, vix_df, input_data):
    strategy = 'SPY PUT CREDIT SPREAD'
    current_input_data = input_data[input_data['Strategy'] == strategy]

    while True:  # datetime.datetime.strptime('2022-02-01', "%Y-%m-%d")

        tick = current_input_data.Stock.values[0]
        atm_put_position = current_input_data.Long.values[0]
        atm_put_1_abobe_position = current_input_data.Short.values[0]

        stock_price_df_native = yf.download(tick)
        sma_20, sma_100, rsi = get_tech_data(stock_price_df_native)

        contract = Stock(tick, 'SMART', 'USD')

        print(f'------- {tick} --------')

        bars = ib.reqHistoricalData(
            contract, endDateTime='', durationStr='365 D',
            barSizeSetting='1 day', whatToShow='OPTION_IMPLIED_VOLATILITY', useRTH=True)

        print(bars)
        df_iv = util.df(bars)

        # bars_hist = ib.reqHistoricalData(
        #     contract, endDateTime='', durationStr='365 D',
        #     barSizeSetting='1 day', whatToShow='HISTORICAL_VOLATILITY', useRTH=True)
        #
        # hist_volatility = util.df(bars_hist)['close']

        print(df_iv.columns.tolist())
        df_iv['IV_percentile'] = df_iv['close'].rolling(364).apply(
            lambda x: stats.percentileofscore(x, x.iloc[-1]))

        print('IV_percentile', df_iv['IV_percentile'].iloc[-1])

        # df_iv['IV_percentile'].iloc[-1] = 90

        # получение айдишника тикера
        bars_id = ib.qualifyContracts(contract)

        df_bars_id = util.df(bars_id)
        ticker_id = df_bars_id['conId'].values[0]
        print(df_bars_id)
        print(ticker_id)

        spx = Index(conId=ticker_id, symbol=tick, exchange='SMART', currency='USD', localSymbol=tick)
        # spx = Index('SPY', 'CBOE')
        ib.qualifyContracts(spx)

        ib.reqMarketDataType(1)
        # 1 = Live
        # 2 = Frozen
        # 3 = Delayed
        # 4 = Delayed frozen

        [ticker] = ib.reqTickers(spx)

        print('ticker', ticker)

        current_price = ticker.marketPrice()



        print('current_price', current_price)

        if current_price > sma_100 and sma_20 > sma_100:
            if 30 < rsi < 70:
                if df_iv['IV_percentile'].iloc[-1] > 50:
                    vix_signal = market_stage_vix(vix_df)
                    if vix_signal <= 2:

                        condition = [f'IV_percentile {df_iv["IV_percentile"].iloc[-1]} > 50, vix_signal =={vix_signal}'
                                     f'RSI: {rsi}, price: {current_price} > sma_100: {sma_100},  sma_20: {sma_20} > sma_100: {sma_100}']

                        chains = ib.reqSecDefOptParams(spx.symbol, '', spx.secType, spx.conId)

                        chain = next(c for c in chains if c.tradingClass == tick and c.exchange == 'SMART')

                        expirations_filter_list_date = []
                        expirations_filter_list_strike = []
                        print('0')

                        # фильтрация будущих контрактов по времени
                        for exp in chain.expirations:

                            year = exp[:4]
                            month = exp[4:6]
                            day = exp[6:]
                            date = year + '-' + month + '-' + day
                            datime_date = datetime.datetime.strptime(date, "%Y-%m-%d")

                            if datime_date > datetime.datetime.now() + relativedelta(
                                    days=25) and datime_date < datetime.datetime.now() + relativedelta(days=60):
                                expirations_filter_list_date.append(exp)

                        print('expirations_filter_list_date', expirations_filter_list_date)

                        print('strikes', chain.strikes)
                        print('expirations', chain.expirations)
                        # фильтрация страйков относительно текущей цены
                        time.sleep(4)

                        for strikus in chain.strikes:
                            if strikus > current_price * 0.5 and strikus < current_price * 1.5:
                                expirations_filter_list_strike.append(strikus)

                        # nearest_equal(chain.strikes.tolist(), current_price)
                        #
                        # expirations_filter_list_strike.append(nearest_equal(chain.strikes.tolist(), current_price))

                        print('expirations_filter_list_strike', expirations_filter_list_strike)

                        time.sleep(4)

                        rights = ['P']

                        contracts = [Option(tick, expiration, strike, right, 'SMART', tradingClass=tick)
                                     for right in rights
                                     for expiration in [expirations_filter_list_date[0]]
                                     # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                                     for strike in expirations_filter_list_strike]

                        contracts = ib.qualifyContracts(*contracts)

                        tickers = ib.reqTickers(*contracts)
                        print('tickers')
                        print(tickers)
                        # df_chains = util.df(tickers)
                        df_chains = chain_converter(tickers)

                        # фильтруем по ликвидности
                        # df_chains = df_chains[df_chains['volume'] > 0]
                        # df_chains.to_excel('chains.xlsx')

                        atm_strike = nearest_equal(df_chains['Strike'].tolist(), current_price)
                        atm_put = df_chains[df_chains['Strike'] == atm_strike].reset_index(drop=True).iloc[0]

                        df_chains_local = df_chains.sort_values('Strike', ascending=True).reset_index(drop=True)
                        atm_1_above_strike = \
                        df_chains_local[df_chains_local['Strike'] > atm_strike].reset_index(drop=True).iloc[0]

                        contract_to_buy = atm_1_above_strike['contract']
                        contract_to_sell = atm_put['contract']

                        # создаем теоретическую позицию для проверки на наличие такой же уже открытой

                        theoretical_position = pd.DataFrame()
                        theoretical_position['contract'] = [contract_to_buy, contract_to_sell]
                        print('-' * 100)
                        print('theoretical_position')
                        print(theoretical_position)

                        # дропаем тикер если такая позиция уже открыта
                        if check_to_open(theoretical_position, strategy, tick):
                            break

                        else:
                            # print(fdasfas)
                            print('-' * 100)
                            print('contract_to_buy', contract_to_buy)
                            print('contract_to_sell', contract_to_sell)

                            # ---- время закрытия позиции

                            exp_exp_date_buy = datetime.datetime.strptime(contract_to_buy.lastTradeDateOrContractMonth,
                                                                          "%Y%m%d")
                            days_to_exp = (datetime.datetime.strptime(contract_to_buy.lastTradeDateOrContractMonth,
                                                                      "%Y%m%d") - datetime.datetime.now()).days
                            time_to_exp_buy = exp_exp_date_buy - relativedelta(days=int(days_to_exp / 2))
                            print('time_to_exp_buy')

                            exp_contract_to_sell = datetime.datetime.strptime(
                                contract_to_sell.lastTradeDateOrContractMonth, "%Y%m%d")
                            days_to_exp = (datetime.datetime.strptime(contract_to_sell.lastTradeDateOrContractMonth,
                                                                      "%Y%m%d") - datetime.datetime.now()).days
                            time_to_exp_sell = exp_contract_to_sell - relativedelta(days=int(days_to_exp / 2))
                            print('time_to_exp_sell')

                            # чекаем последний айдишник сложных позиций
                            try:
                                MAIN_LOG_FILE = pd.read_csv('LOGING_FILES/LOG_FILE.csv').reset_index(drop=True)
                                hard_position = MAIN_LOG_FILE['hard_position'].max() + 1
                            except:
                                hard_position = 1

                            order_buy = MarketOrder("Buy", atm_put_position)
                            trade_buy = ib.placeOrder(contract_to_buy, order_buy)
                            while not trade_buy.isDone():
                                ib.waitOnUpdate()
                            ib.sleep(5)
                            await asyncio.sleep(5)
                            logg_df = logging_open(trade_buy, contract_to_buy, order_buy, strategy, hard_position, ib,
                                                   condition, time_to_exp_buy)

                            order_sell = MarketOrder("Sell", atm_put_1_abobe_position)
                            trade_sell = ib.placeOrder(contract_to_sell, order_sell)
                            while not trade_sell.isDone():
                                ib.waitOnUpdate()
                            ib.sleep(15)
                            await asyncio.sleep(5)
                            print('shares')
                            print(atm_put_position)
                            print(atm_put_1_abobe_position)

                            logg_df = logging_open(trade_sell, contract_to_sell, order_sell, strategy, hard_position,
                                                   ib, condition, time_to_exp_sell)
                            print('end while')

                            post_loging_calc()

        await asyncio.sleep(30)

        check_time_signal = check_time()
        print('check_time')
        print(check_time_signal)
        # проверка времени выполнения цикла
        if check_time_signal != 'work_time':
            print('ZZZZZZZZZZZZzzzzzzzzzzzzzzzzzzzz')
            await asyncio.sleep(3600)
