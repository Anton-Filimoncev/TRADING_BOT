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

positions = ib.positions()  # A list of positions, according to IB

position_df = util.df(positions, labels=None)
print(position_df)
position_df.to_excel('position_df.xlsx')

def check_time():
    current_time = datetime.datetime.now().time()
    if current_time < datetime.time(23, 59, 0) and current_time > datetime.time(16, 30, 0):
        return 'work_time'
    else:
        return 'sleep_time'

def nearest_equal(lst, target):
    return min(lst, key=lambda x: abs(x - target))


def logging(trade, contract_to_buy, order, strategy):

    print('trade')
    print(trade)
    print('contract_to_buy')
    print(contract_to_buy)
    print(util.fields(contract_to_buy))
    print(util.dataclassAsTuple(contract_to_buy))
    print('order')
    print(order)


    execution = pd.DataFrame([util.dataclassAsDict(trade.fills[0].execution)]).drop(['execId'], axis=1)
    contract = pd.DataFrame([util.dataclassAsDict(trade.fills[0].contract)]).drop(['exchange', 'currency'], axis=1) #, ,
    commission = pd.DataFrame([util.dataclassAsDict(trade.fills[0].commissionReport)])#
    time_trade = trade.fills[0].time
    whatif = ib.whatIfOrder(contract_to_buy, order)
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
    execution['contract'] = contract_to_buy
    execution['strategy'] = strategy

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

    except:
        MAIN_LOG_FILE = pd.DataFrame()
        OPEN_POSITIONS = pd.DataFrame()
        pass
    # print('current_open_hard_position')
    # print(current_open_hard_position.columns)
    print('MAIN_LOG_FILE')
    print(MAIN_LOG_FILE.columns)

    MAIN_LOG_FILE = pd.concat([logg_df, MAIN_LOG_FILE], axis=0, ignore_index=True).reset_index(drop=True)
    OPEN_POSITIONS = pd.concat([logg_df, OPEN_POSITIONS], axis=0, ignore_index=True).reset_index(drop=True)
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
    OPEN_POSITIONS.to_pickle('LOGING_FILES/OPEN_POSITIONS.pkl')

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

        tick = 'MMM'

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
                            days=60) and datime_date < datetime.datetime.now() + relativedelta(days=90):
                        expirations_filter_list_date.append(exp)

                print('expirations_filter_list_date', expirations_filter_list_date)

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

                # print(fdasfas)
                print('-' * 100)
                print('contract_to_buy', contract_to_buy)
                print('contract_to_sell', contract_to_sell)

                conid1 = ib.qualifyContracts(contract_to_buy)[0].conId
                print(conid1)
                conid2 = ib.qualifyContracts(contract_to_sell)[0].conId
                print(conid2)

                contract = Contract(conId=ticker_id, symbol=tick, exchange='SMART', currency='USD', localSymbol=tick, comboLegs=[
                    ComboLeg(conId=conid1, ratio=1, action='BUY'),
                    ComboLeg(conId=conid2, ratio=1, action='BUY')
                ])
                print('contract')
                print(contract)

                order_new = MarketOrder("Buy", atm_call_position)
                trade_new = ib.placeOrder(contract, order_new)
                while not trade_new.isDone():
                    ib.waitOnUpdate()
                IB.sleep(1)

                log_df = logging(trade_new, contract, order_new, strategy)


                #
                # order_new = MarketOrder("Sell", atm_call_position)
                # trade_new = ib.placeOrder(contract, order_new)
                # while not trade_new.isDone():
                #     ib.waitOnUpdate()
                #
                # log_df = logging(trade_new, contract, order_new)

                check_time_signal = check_time()

                # проверка времени выполнения цикла
                if check_time_signal != 'work_time':
                    time.sleep(3600)


                # print('start_date', start_date)
                #
                # try:
                #     percentile = volatility_df[volatility_df['Date'] == start_date]['Percentile']
                #     percentile = float(percentile)
                # except:
                #     percentile = np.NAN
                #
                # print('percentile', percentile)
                #
                # if percentile <= 50:
                #
                #     vix_signal = market_stage_vix(start_date, vix_df)
                #
                #     print('vix_signal', vix_signal)
                #
                #     if vix_signal == 1:
                #
                #         try:
                #             stock_price_df = stock_price_df_native[start_date:]
                #             stock_price_df['updated'] = stock_price_df.index.tolist()
                #             stock_price_df['updated'] = stock_price_df['updated'].dt.strftime('%Y-%m-%d')
                #
                #             current_price = stock_price_df['Close'][0]
                #             print('current_price: ', current_price)
                #
                #
                #             all_contract_df = get_all_contract(start_date, ticker)
                #             side = 'call'
                #
                #
                #             limit_date_expiration_min = start_date + relativedelta(days=+90)
                #             limit_date_expiration_max = start_date + relativedelta(days=+120)
                #
                #             # ------------------------------------  Продажа АТМ кола
                #
                #             all_contract_df_filtered_date_sell = contract_filter_date(all_contract_df, limit_date_expiration_min, limit_date_expiration_max, side)
                #
                #
                #             needed_strike_sell = get_needed_strike(all_contract_df_filtered_date_sell, current_price, 'call_sell')
                #
                #             # print('all_contract_df_filtered_put_sell')
                #             print(all_contract_df_filtered_date_sell['strike'])
                #             print(needed_strike_sell)
                #             all_contract_df_filtered_sell = contract_filter_strike(all_contract_df_filtered_date_sell, needed_strike_sell)
                #             # print('option_history_chain_put_sell')
                #             option_history_chain_sell = get_optons_chain(all_contract_df_filtered_sell, needed_strike_sell, stock_price_df)
                #
                #
                #             # ------------------------------------  Покупка колы на 4 страйк выше
                #             # print('all_contract_df_filtered_date_put_buy')
                #             all_contract_df_filtered_date_buy = contract_filter_date(all_contract_df, limit_date_expiration_min,
                #                                                                           limit_date_expiration_max, side)
                #             # print('needed_strike_put_buy')
                #             needed_strike_buy = get_needed_strike(all_contract_df_filtered_date_buy, needed_strike_sell, 'call_buy')
                #             # print('all_contract_df_filtered_put_buy')
                #
                #             print(all_contract_df_filtered_date_buy['strike'])
                #             print(needed_strike_buy)
                #             all_contract_df_filtered_buy = contract_filter_strike(all_contract_df_filtered_date_buy, needed_strike_buy)
                #             # print('option_history_chain_put_buy')
                #             option_history_chain_buy = get_optons_chain(all_contract_df_filtered_buy, needed_strike_buy, stock_price_df)
                #
                #

#
# MAIN_LOG_FILE = pd.read_csv('LOG_FILE.csv')
# log_df = logging(trade, contract_to_buy, order)
#
# MAIN_LOG_FILE = pd.concat([MAIN_LOG_FILE, log_df], axis=0)
# print(log_df)
#
# print(ib.whatIfOrder(contract_to_buy, order))
# print('-')
# print(sum(fill.commissionReport.commission for fill in ib.fills()))
#
# # order = MarketOrder('BUY', 1)
# # trade = ib.placeOrder(stock, order)
#
#
# MAIN_LOG_FILE.to_csv('LOG_FILE.csv', index=False)
