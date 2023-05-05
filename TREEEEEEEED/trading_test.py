import pandas as pd
import numpy as np
from ib_insync import *
import asyncio
import scipy.stats as stats
import datetime
import time
from dateutil.relativedelta import relativedelta
from LOGING_FILES.LOGING import *


# import nest_asyncio
# nest_asyncio.apply()
# __import__('IPython').embed()

# async def position1(ibb):
#     while True:
#         await asyncio.sleep(6)
#         positions = ibb.positions()  # A list of positions, according to IB
#         position_df = util.df(positions, labels=None)
#         print('position_df1')
#         print(position_df)
#
#
# async def position2(ibb):
#     while True:
#         await asyncio.sleep(2)
#         positions = ibb.positions()  # A list of positions, according to IB
#         position_df = util.df(positions, labels=None)
#
#         print('position_df2')
#         print(position_df)
#
#
# async def main():
#
#     ib = IB()
#     try:
#         ib.connect('127.0.0.1', 4002, clientId=12)  # 7497
#
#     except:
#         ib.connect('127.0.0.1', 7496, clientId=12)
#
#     tasks = [
#         position1(ib),
#         position2(ib)
#     ]
#
#     await asyncio.gather(*tasks)
#
#
# asyncio.run(main())

# post_loging_calc()

OPEN_POSITIONS = pd.read_pickle('LOGING_FILES/OPEN_POSITIONS.pkl')
#
# print(OPEN_POSITIONS.expected_return)
# print(OPEN_POSITIONS.hard_position)

#
# while not trade.isDone():
#     print(trade.order)
#
#     ib.waitOnUpdate()
# ib.sleep(15)

# trade.fills = [0]
# post_loging_calc()
# LOG_FILE = pd.read_csv('LOG_FILE.csv')
# LOG_FILE2 = pd.read_csv('LOGING_FILES/LOG_FILE.csv')
# print(LOG_FILE.price)
# print(LOG_FILE.expected_return)
# print(LOG_FILE2.expected_return)




print()

#
timus = datetime.datetime.strptime(OPEN_POSITIONS.lastTradeDateOrContractMonth[0], "%Y%m%d") - relativedelta(days=1)

# (timus - OPEN_POSITIONS.time[0]).days
print(int((datetime.datetime.strptime(OPEN_POSITIONS.lastTradeDateOrContractMonth[0], "%Y%m%d") - datetime.datetime.now()).days / 3))
print(datetime.datetime.now().date())
print(datetime.datetime.now().date() == datetime.datetime.strptime(OPEN_POSITIONS.lastTradeDateOrContractMonth[0], "%Y%m%d") - relativedelta(days=57))