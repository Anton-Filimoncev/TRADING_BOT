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
from STRAT.SPY_CALL_DEBET_SPREAD import spy_call_debet_spread_strat
from STRAT.SPY_PUT_CREDIT_SPREAD import spy_put_credit_spread_strat
from STRAT.VIX_CALL_BACKSPREAD_HEDGE import vix_call_backspread_hedge
from STRAT.VIX_SHORT_PUT import vix_short_put
# from LOGING_FILES.LOGING import logging_open
from close_position import check_to_close
# from support import *
import nest_asyncio
nest_asyncio.apply()
import threading


from contextvars import ContextVar

ib = IB()
try:
    ib.connect('127.0.0.1', 4002, clientId=12)  # 7496

except:
    ib.connect('127.0.0.1', 7497, clientId=12)

async def run(tasks):
    tasks = tasks
    await asyncio.gather(tasks)

def start(tasks):

    asyncio.run(run(tasks))

def run_threads(tasks):
    threads = [threading.Thread(target=start, args=(task,)) for task in tasks]
    for thread in threads:
        thread.start()  # каждый поток должен быть запущен
    for thread in threads:
        thread.join()  # дожидаемся исполнения всех потоков



tasks = [
    spy_call_debet_spread_strat(ib),
    spy_put_credit_spread_strat(ib),
    vix_call_backspread_hedge(ib),
    vix_short_put(ib),
    check_to_close(ib),
]


run_threads(tasks)





# call_debet_spread_tickers = ['SPY']
#
# # REMX , GDX  KWEB  GDXJ
#
# atm_call_position = [1]
# atm_call_1_above_position = [1]
#
# call_debet_spread_rule = pd.DataFrame(
#     {
#         'Stock': call_debet_spread_tickers,
#         'ATM_Call_Position': atm_call_position,
#         'ATM_Call_1_Above_Position': atm_call_1_above_position
#     }
# )
# # IB.run(call_debet_spread_strat(call_debet_spread_rule, ib))
#
#
#
#
# put_credit_spread_tickers = ['SPY']
# ATM_PUT_Position = [1]
# ATM_PUT_1_above_position = [1]
#
# put_credit_spread_rule = pd.DataFrame(
#     {
#         'Stock': put_credit_spread_tickers,
#         'ATM_PUT_Position': ATM_PUT_Position,
#         'ATM_PUT_1_above_position': ATM_PUT_1_above_position
#     }
# )
#
#
# vix_call_backspread_hedge_tickers = ['VIX']
# ATM_VIX_short_Position = [5]
# ATM_CALL_5_above_position = [10]
# vix_call_backspread_hedge_rule = pd.DataFrame(
#     {
#         'Stock': vix_call_backspread_hedge_tickers,
#         'ATM_VIX_short_Position': ATM_VIX_short_Position,
#         'ATM_CALL_5_above_position': ATM_CALL_5_above_position
#     }
# )
#
# vix_short_put_tickers = ['VIX']
# ATM_VIX_put_short_Position = [5]
# vix_short_put_rule = pd.DataFrame(
#     {
#         'Stock': vix_call_backspread_hedge_tickers,
#         'ATM_VIX_short_Position': ATM_VIX_put_short_Position,
#     }
# )



# asyncio.run(run())