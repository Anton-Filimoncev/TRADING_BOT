import threading
import sys
import time
from ib_insync import *



ib = IB()
ib.connect('127.0.0.1', 7497, clientId=12)
# ib.disconnect()

tick = 'VIX'

contract = Stock(tick, 'CBOE', 'USD')
# contract.exchange = 'CBOE'
contract.secType = "IND"
#

print(contract)
print(f'~~~~~ {tick} ~~~~~')

bars = ib.reqHistoricalData(
    contract, endDateTime='', durationStr='365 D',
    barSizeSetting='1 day', whatToShow='OPTION_IMPLIED_VOLATILITY', useRTH=True)

print(bars)
df_iv = util.df(bars)

bars_id = ib.qualifyContracts(contract)
print(bars_id)
df_bars_id = util.df(bars_id)
ticker_id = df_bars_id['conId'].values[0]

print('ticker_id', ticker_id)

spx = Index(conId=ticker_id, symbol=tick, exchange='CBOE', currency='USD', localSymbol=tick)
# spx = Index('SPY', 'CBOE')
spx.secType = "IND"
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