import pandas as pd
import numpy as np
from ib_insync import *
import asyncio
import scipy.stats as stats
import datetime
import time
from dateutil.relativedelta import relativedelta
from VIX_market_stage import market_stage_vix
import yfinance as yf
import pickle

ib = IB()
try:
    ib.connect('127.0.0.1', 4002, clientId=12)  # 7497

except:
    ib.connect('127.0.0.1', 7496, clientId=12)


def check_time():
    current_time = datetime.datetime.now().time()
    if current_time < datetime.time(23, 59, 0) and current_time > datetime.time(16, 30, 0):
        return 'work_time'
    else:
        return 'sleep_time'

def nearest_equal(lst, target):
    return min(lst, key=lambda x: abs(x - target))


def logging_open(trade, contract_to_pkl, order, strategy, hard_position):

    print('trade')
    print(trade)
    print('contract_to_buy')
    print(contract_to_pkl)
    print(util.fields(contract_to_pkl))
    print(util.dataclassAsTuple(contract_to_pkl))
    print('order')
    print(order)


    execution = pd.DataFrame([util.dataclassAsDict(trade.fills[0].execution)]).drop(['execId'], axis=1)
    contract = pd.DataFrame([util.dataclassAsDict(trade.fills[0].contract)]).drop(['exchange', 'currency'], axis=1) #, ,
    commission = pd.DataFrame([util.dataclassAsDict(trade.fills[0].commissionReport)])#
    time_trade = trade.fills[0].time
    whatif = ib.whatIfOrder(contract_to_pkl, order)
    state = pd.DataFrame([util.dataclassAsDict(whatif)])
    state = state[['initMarginBefore', 'maintMarginBefore', 'equityWithLoanBefore', 'initMarginChange', 'maintMarginChange',
                   'equityWithLoanChange', 'initMarginAfter', 'maintMarginAfter', 'equityWithLoanAfter']]
    print('whatif')
    print(whatif)
    print('execution')
    print(trade.fills[0].execution)
    print('contract')
    print(trade.fills[0].contract)
    print('commission')
    print(trade.fills[0].commissionReport)
    # print(commission)
    # print(commission)
    # print(state)


    execution['time'] = execution['time'].dt.tz_convert(tz='Europe/Moscow').dt.tz_localize(None)
    execution['contract'] = contract_to_pkl
    execution['strategy'] = strategy
    execution['hard_position'] = hard_position
    execution['status'] = 'OPEN'

    # execution['hard_position'] = hard_position

    logg_df = pd.concat([execution, contract, commission, state], axis=1)

    print('maintMarginChange')
    print(logg_df['maintMarginChange'].values[0])

    if strategy == 'SP500 CALL DEBET SPREAD_v01':
        logg_df['expected_return'] = float(logg_df['maintMarginChange'].values[0]) * 0.2

    print('logg_df')
    print(logg_df)

    # current_open_hard_position = pd.concat([log_df_buy, log_df_sell], axis=0,
    #                                        ignore_index=True).reset_index(drop=True)
    try:
        MAIN_LOG_FILE = pd.read_csv('LOGING_FILES/LOG_FILE.csv').reset_index(drop=True)
        MAIN_LOG_FILE.to_csv('LOGING_FILES/LOG_FILE_backup.csv', index=False)
        OPEN_POSITIONS = pd.read_csv('LOGING_FILES/OPEN_POSITIONS.csv').reset_index(drop=True)
        OPEN_POSITIONS.to_csv('LOGING_FILES/OPEN_POSITIONS_backup.csv', index=False)
        OPEN_POSITIONS_pkl = pd.read_pickle('LOGING_FILES/OPEN_POSITIONS.pkl').reset_index(drop=True)
    except:
        MAIN_LOG_FILE = pd.DataFrame()
        OPEN_POSITIONS = pd.DataFrame()
        OPEN_POSITIONS_pkl = pd.DataFrame()
        pass
    # print('current_open_hard_position')
    # print(current_open_hard_position.columns)
    print('MAIN_LOG_FILE')
    print(MAIN_LOG_FILE.columns)

    MAIN_LOG_FILE = pd.concat([logg_df, MAIN_LOG_FILE], axis=0, ignore_index=True).reset_index(drop=True)
    OPEN_POSITIONS = pd.concat([logg_df, OPEN_POSITIONS], axis=0, ignore_index=True).reset_index(drop=True)
    OPEN_POSITIONS_pkl = pd.concat([logg_df, OPEN_POSITIONS_pkl], axis=0, ignore_index=True).reset_index(drop=True)
    print(MAIN_LOG_FILE)
    print('OPEN_POSITIONS')
    print(OPEN_POSITIONS)

    # print(ib.whatIfOrder(contract_to_buy, order))
    # print('-')
    # print(sum(fill.commissionReport.commission for fill in ib.fills()))

    # order = MarketOrder('BUY', 1)
    # trade = ib.placeOrder(stock, order)

    MAIN_LOG_FILE.to_csv('LOGING_FILES/LOG_FILE.csv', index=False)
    MAIN_LOG_FILE.to_pickle('LOGING_FILES/LOG_FILE_backup.pkl')
    OPEN_POSITIONS.to_csv('LOGING_FILES/OPEN_POSITIONS.csv', index=False)
    OPEN_POSITIONS_pkl.to_pickle('LOGING_FILES/OPEN_POSITIONS.pkl')

    return logg_df


print()


def chain_converter(tickers):
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

    # print(tickers.modelGreeks.delta)
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
        # else:
        #     print("modelGreeks not yet available")

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

def check_to_open(theoretical_position):
    try:
        OPEN_POSITIONS = pd.read_csv('LOGING_FILES/OPEN_POSITIONS.csv').reset_index(drop=True)

        postion_is_open = True

        for hard_position in OPEN_POSITIONS.hard_position.unique():
            print('========')
            print('hard_position')
            print(hard_position)
            OPEN_POSITIONS_local = OPEN_POSITIONS[OPEN_POSITIONS['hard_position'] == hard_position][::-1].reset_index(drop=True)

            print(OPEN_POSITIONS_local['contract'].equals(theoretical_position['contract'].astype('str')))
            print('========')
            if OPEN_POSITIONS_local['contract'].equals(theoretical_position['contract'].astype('str')):
                postion_is_open = False
                print(OPEN_POSITIONS['contract'].equals(theoretical_position['contract']))

                print('position is already open')
                print('PPPAAASSSS')
    except:
        postion_is_open = True

    return postion_is_open

if __name__ == '__main__':

    ticker = 'SPY'

    strategy = 'SP500 CALL DEBET SPREAD_v01'

    # stock_price_df_native = yf.download(ticker)
    vix_df = yf.download('^VIX')

    date_month_list = []
    cum_pnl = 0

    position_num = 10

    start_date = datetime.datetime.now()
    end_date = datetime.datetime.strptime('2023-01-01', '%Y-%m-%d')
    df_to_excel = pd.DataFrame()

    while True:  # datetime.datetime.strptime('2022-02-01', "%Y-%m-%d")

        tickers = ['MMM', 'SPY', 'AAPL']

        for tick in tickers:

            contract = Stock(tick, 'SMART', 'USD')

            print('---------------')

            bars = ib.reqHistoricalData(
                contract, endDateTime='', durationStr='365 D',
                barSizeSetting='1 day', whatToShow='OPTION_IMPLIED_VOLATILITY', useRTH=True)

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

            df_iv['IV_percentile'].iloc[-1] = 90

            if df_iv['IV_percentile'].iloc[-1] >= 50:

                # vix_signal = market_stage_vix(start_date, vix_df)
                vix_signal = 1
                print('vix_signal', vix_signal)

                if vix_signal == 1:

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
                                days=60) and datime_date < datetime.datetime.now() + relativedelta(days=100):
                            expirations_filter_list_date.append(exp)

                    print('expirations_filter_list_date', expirations_filter_list_date)

                    print('strikes',  chain.strikes)
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

                    rights = ['C']

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
                    df_chains.to_excel('chains.xlsx')

                    atm_strike = nearest_equal(df_chains['Strike'].tolist(), current_price)

                    atm_call = df_chains[df_chains['Strike'] == atm_strike].reset_index(drop=True).iloc[0]

                    atm_call_5_abowe = df_chains[df_chains['Strike'] > atm_strike].reset_index(drop=True).iloc[4]

                    contract_to_buy = atm_call['contract']
                    contract_to_sell = atm_call_5_abowe['contract']

                    atm_call_position = 10
                    atm_call_5_abowe_position = 10

                    # создаем теоретическую позицию для проверки на наличие такой же уже открытой

                    theoretical_position = pd.DataFrame()
                    theoretical_position['contract'] = [contract_to_buy, contract_to_sell]
                    print('-' * 100)
                    print('theoretical_position')
                    print(theoretical_position)

                    # дропаем тикер если такая позиция уже открыта
                    if check_to_open(theoretical_position):
                        pass

                        # print(fdasfas)
                        print('-' * 100)
                        print('contract_to_buy', contract_to_buy)
                        print('contract_to_sell', contract_to_sell)

                        # чекаем последний айдишник сложных позиций
                        try:
                            MAIN_LOG_FILE = pd.read_csv('LOGING_FILES/LOG_FILE.csv').reset_index(drop=True)
                            hard_position = MAIN_LOG_FILE['hard_position'].max() + 1
                        except:
                            hard_position = 1

                        order_buy = MarketOrder("Buy", atm_call_position)
                        trade_buy = ib.placeOrder(contract_to_buy, order_buy)
                        while not trade_buy.isDone():
                            ib.waitOnUpdate()
                        IB.sleep(1)

                        log_df = logging(trade_buy, contract_to_buy, order_buy, strategy, hard_position)

                        order_sell = MarketOrder("Sell", atm_call_5_abowe_position)
                        trade_sell = ib.placeOrder(contract_to_sell, order_sell)
                        while not trade_sell.isDone():
                            ib.waitOnUpdate()
                        IB.sleep(1)

                        log_df = logging(trade_sell, contract_to_sell, order_sell, strategy, hard_position)
                        print('end while')

        check_time_signal = check_time()
        print('check_time')
        print(check_time_signal)
        # проверка времени выполнения цикла
        if check_time_signal == 'work_time':
            print('ZZZZZZZZZZZZzzzzzzzzzzzzzzzzzzzz')
            time.sleep(3600)

