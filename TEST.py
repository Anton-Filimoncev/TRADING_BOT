import threading
import sys
import time
from ib_insync import *
import pandas as pd
import datetime

OPEN_POSITIONS_pkl = pd.read_pickle('LOGING_FILES/LOG_FILE_backup.pkl').reset_index(drop=True)

print(OPEN_POSITIONS_pkl.contract[0])
print(OPEN_POSITIONS_pkl.contract[1])
print(OPEN_POSITIONS_pkl.contract[2])
print(OPEN_POSITIONS_pkl.contract[3])
# OPEN_POSITIONS_pkl.to_pickle('LOGING_FILES/OPEN_POSITIONS.pkl')
# print(type(Option(conId=584544938, symbol='SPY', lastTradeDateOrContractMonth='20230616', strike=414.0, right='C', multiplier='100', exchange='SMART', currency='USD', localSymbol='SPY   230616C00414000', tradingClass='SPY')))
#
#
# OPEN_POSITIONS_pkl.to_csv('LOGING_FILES/LOG_FILE.csv', index=False)
# OPEN_POSITIONS_pkl.to_csv('LOGING_FILES/OPEN_POSITIONS.csv', index=False)
#
# MAIN_LOG_FILE = pd.read_csv('LOGING_FILES/LOG_FILE.csv').reset_index(drop=True)


OPEN_POSITIONS_pkl.contract[0] = Option(conId=584544938, symbol='SPY', lastTradeDateOrContractMonth='20230616', strike=414.0, right='C', multiplier='100', exchange='SMART', currency='USD', localSymbol='SPY   230616C00414000', tradingClass='SPY')
OPEN_POSITIONS_pkl.contract[1] = Option(conId=584544924, symbol='SPY', lastTradeDateOrContractMonth='20230616', strike=413.0, right='C', multiplier='100', exchange='SMART', currency='USD', localSymbol='SPY   230616C00413000', tradingClass='SPY')
OPEN_POSITIONS_pkl.contract[2] = Option(conId=588771516, symbol='VIX', lastTradeDateOrContractMonth='20230620', strike=17.0, right='C', multiplier='100', exchange='SMART', currency='USD', localSymbol='VIX   230621C00017000', tradingClass='VIX')
OPEN_POSITIONS_pkl.contract[3] = Option(conId=588771575, symbol='VIX', lastTradeDateOrContractMonth='20230620', strike=22.0, right='C', multiplier='100', exchange='SMART', currency='USD', localSymbol='VIX   230621C00022000', tradingClass='VIX')

OPEN_POSITIONS_pkl.date_to_close[0] = datetime.datetime.strptime(OPEN_POSITIONS_pkl.date_to_close[0].split(' ')[0], "%Y-%m-%d")
OPEN_POSITIONS_pkl.date_to_close[1] = datetime.datetime.strptime(OPEN_POSITIONS_pkl.date_to_close[1].split(' ')[0], "%Y-%m-%d")
OPEN_POSITIONS_pkl.date_to_close[2] = datetime.datetime.strptime(OPEN_POSITIONS_pkl.date_to_close[2].split(' ')[0], "%Y-%m-%d")
OPEN_POSITIONS_pkl.date_to_close[3] = datetime.datetime.strptime(OPEN_POSITIONS_pkl.date_to_close[3].split(' ')[0], "%Y-%m-%d")


OPEN_POSITIONS_pkl.time[0] = datetime.datetime.strptime(OPEN_POSITIONS_pkl.time[0].split(' ')[0], "%Y-%m-%d")
OPEN_POSITIONS_pkl.time[1] = datetime.datetime.strptime(OPEN_POSITIONS_pkl.time[1].split(' ')[0], "%Y-%m-%d")
OPEN_POSITIONS_pkl.time[2] = datetime.datetime.strptime(OPEN_POSITIONS_pkl.time[2].split(' ')[0], "%Y-%m-%d")
OPEN_POSITIONS_pkl.time[3] = datetime.datetime.strptime(OPEN_POSITIONS_pkl.time[3].split(' ')[0], "%Y-%m-%d")

# POSITION.time


print(type(OPEN_POSITIONS_pkl.date_to_close[0]))
OPEN_POSITIONS_pkl.to_pickle('LOGING_FILES/OPEN_POSITIONS.pkl')