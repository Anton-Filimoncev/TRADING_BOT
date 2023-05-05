import pandas as pd
import numpy as np
from ib_insync import *
import asyncio
import time
import math

ib = IB()
try:
    ib.connect('127.0.0.1', 4002, clientId=12)  # 7497

except:
    ib.connect('127.0.0.1', 7496, clientId=12)

def check_pnl(account, conId, ):

    tempObj = ib.reqPnLSingle(account,  modelCode='', conId=conId)

    while math.isnan(tempObj.realizedPnL):
        # print(tempObj.realizedPnL)
        # print(tempObj)
        ib.waitOnUpdate()
    print(tempObj)
    ib.cancelPnLSingle(account, modelCode='', conId=conId)

    return tempObj


print(check_pnl('DU3624435',  conId=9720).unrealizedPnL)