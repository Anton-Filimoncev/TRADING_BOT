import pandas as pd
import numpy as np
from ib_insync import *
import asyncio
import time
import math
import ast
from LOGING_FILES.LOGING import logging_close
from support import *
import yfinance as yf
from VIX_market_stage import market_stage_vix


async def check_to_close(ib, vix_df, yahoo_stock):
    await asyncio.sleep(30)
    # while True:
    # try:
    OPEN_POSITIONS = pd.read_pickle('LOGING_FILES/OPEN_POSITIONS.pkl')

    print('OPEN_POSITIONS')
    print(OPEN_POSITIONS.hard_position.unique())

    vix_signal = market_stage_vix(vix_df)


    for hard_position in OPEN_POSITIONS.hard_position.unique():
        # print('hard_position')
        # print(hard_position)
        # фрейм из одной сложной позиции
        OPEN_POSITIONS_position = OPEN_POSITIONS[OPEN_POSITIONS['hard_position'] == hard_position]
        # print(OPEN_POSITIONS_position)
        postition_pnl = 0

        for indexus, POSITION_pnl_check in OPEN_POSITIONS_position.iterrows():

            # одна позиция из сложной позиции

            if POSITION_pnl_check.side == 'SLD':
                what_to_do = 'Buy'
                call_option_ticker = ib.reqMktData(POSITION_pnl_check.contract, "221", False, False)
                ib.sleep(5)
                print('call_option_ticker SLD')
                print(call_option_ticker)
                print(POSITION_pnl_check.contract)
                current_option_price = call_option_ticker.ask
                print('current_option_price')
                print(current_option_price)
                postition_pnl += POSITION_pnl_check.price - current_option_price
            else:
                what_to_do = 'Sell'
                call_option_ticker = ib.reqMktData(POSITION_pnl_check.contract, "221", False, False)
                # ib.sleep(3)
                ib.sleep(5)
                print('call_option_ticker BOT')
                print(call_option_ticker.bid)
                print(POSITION_pnl_check.price)
                current_option_price = call_option_ticker.bid
                postition_pnl += current_option_price - POSITION_pnl_check.price
                print('current_option_price')
                print(current_option_price)

            order = MarketOrder(what_to_do, POSITION_pnl_check.shares)
            whatif = ib.whatIfOrder(POSITION_pnl_check.contract, order)

            print('whatif')
            print(whatif.maxCommission)

            postition_pnl += (- whatif.maxCommission)

        print('---------------------')
        print('postition_pnl', OPEN_POSITIONS_position.symbol)
        print(postition_pnl)

        for index, POSITION in OPEN_POSITIONS_position.iterrows():

            #  одна позиция из сложной позиции

            print('---------------------')
            # print('POSITION.contract')
            # print(POSITION.contract)

            # проверка доходности всей позиции относительно ожидаемой доходности всей позиции (она указана
            # для каждого контракта в общем)

            date_to_close_exp = POSITION.date_to_close

            if postition_pnl >= POSITION.expected_return or datetime.datetime.now().date() == date_to_close_exp or postition_pnl < POSITION.maximum_loss and POSITION.strategy != 'VIX CALL BACKSPREAD HEDGE':

                if POSITION.side == 'SLD':
                    what_to_do = 'Buy'
                else:
                    what_to_do = 'Sell'

                # print(POSITION.side)
                # print(what_to_do)
                order_new = MarketOrder(what_to_do, POSITION.shares)
                # trade_new = ib.placeOrder(POSITION.contract, order_new)
                # print(type(POSITION.contract))
                # print(POSITION.contract)

                IB.sleep(1)
                # contract11 = ib.qualifyContracts(POSITION.contract)
                trade_new = ib.placeOrder(POSITION.contract, order_new)

                # print('-')
                while not trade_new.isDone():
                    ib.waitOnUpdate()
                IB.sleep(10)

                print('POSITION IS CLOSED!')
                # print(POSITION)
                print(POSITION.contract)

                # loging
                logging_close(trade_new, POSITION.contract, order_new, POSITION.strategy, POSITION.conId, POSITION.hard_position, ib, postition_pnl)


            if POSITION.strategy == 'VIX CALL BACKSPREAD HEDGE':
                if (date_to_close_exp - datetime.datetime.now()).days <= (date_to_close_exp - POSITION.time).days / 2 or vix_signal == 3:
                    print('AAAAAA', vix_signal)
                    print((date_to_close_exp - datetime.datetime.now()).days)
                    print((date_to_close_exp - POSITION.time).days / 2)
                    if POSITION.side == 'SLD':
                        what_to_do = 'Buy'
                    else:
                        what_to_do = 'Sell'

                    # print(POSITION.side)
                    # print(what_to_do)
                    order_new = MarketOrder(what_to_do, POSITION.shares)
                    # trade_new = ib.placeOrder(POSITION.contract, order_new)
                    # print(type(POSITION.contract))
                    # print(POSITION.contract)

                    IB.sleep(1)
                    # contract11 = ib.qualifyContracts(POSITION.contract)
                    trade_new = ib.placeOrder(POSITION.contract, order_new)

                    # print('-')
                    while not trade_new.isDone():
                        ib.waitOnUpdate()
                    IB.sleep(10)

                    print('POSITION IS CLOSED!')
                    # print(POSITION)
                    print(POSITION.contract)

                    # loging
                    logging_close(trade_new, POSITION.contract, order_new, POSITION.strategy, POSITION.conId, POSITION.hard_position, ib, postition_pnl)

        #
        # except:
        #     pass

        # await asyncio.sleep(30)
        #
        # check_time_signal = check_time()
        # print('check_time')
        # print(check_time_signal)
        # # проверка времени выполнения цикла
        # if check_time_signal != 'work_time':
        #     print('ZZZZZZZZZZZZzzzzzzzzzzzzzzzzzzzz')
        #     await asyncio.sleep(3600)
