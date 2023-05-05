import pandas as pd
import numpy as np
from ib_insync import *
import asyncio
import scipy.stats as stats
import datetime
import time
from dateutil.relativedelta import relativedelta
from LOGING_FILES.LOGING import post_loging_calc


ib = IB()
try:
    ib.connect('127.0.0.1', 4002, clientId=12)  # 7496

except:
    ib.connect('127.0.0.1', 7497, clientId=12)

positions = ib.positions()  # A list of positions, according to IB
print(util.df(positions))
print(positions[0].contract)
for position in positions:
    print(position.contract)
    print(position.position)
    contract = position.contract
    if position.position > 0: # Number of active Long positions
        action = 'Sell' # to offset the long positions
    elif position.position < 0: # Number of active Short positions
        action = 'Buy' # to offset the short positions



    totalQuantity = abs(position.position)

    print(abs(position.position))
    print(action)
    contract.exchange = 'SMART'
    print(type(contract))
    print(contract)
    order = MarketOrder(action, totalQuantity)
    # IB.sleep(1)
    trade = ib.placeOrder(contract, order)
    while not trade.isDone():
        ib.waitOnUpdate()


    print(f'Flatten Position: {action} {totalQuantity} {contract.localSymbol}')
    assert trade in ib.trades(), 'trade not listed in ib.trades'