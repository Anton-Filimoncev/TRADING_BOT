import pandas as pd
import numpy as np
from ib_insync import *
import time
import pickle


def post_loging_calc():
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

    expected_return_list = []
    maximum_loss_list = []

    for hard_position in MAIN_LOG_FILE.hard_position.unique():
        print('hard_position')
        print(hard_position)
        LOG_POSITIONS = MAIN_LOG_FILE[MAIN_LOG_FILE['hard_position'] == hard_position]
        print(LOG_POSITIONS)

        expected_return_postion = 0
        maximum_loss = 0

        # ТОчки максимального убытка и ожидаемой доходности

        for indexus, POSITION in LOG_POSITIONS.iterrows():
            if POSITION.strategy == 'SPY CALL DEBET SPREAD':
                if POSITION.side == 'SLD':
                    expected_return_postion += (POSITION.price * 100 * POSITION.shares) * 0.25
                    maximum_loss += (POSITION.price * 100 * POSITION.shares) * 0.5
                if POSITION.side == 'BOT':
                    expected_return_postion += -(POSITION.price * 100 * POSITION.shares) * 0.25
                    maximum_loss += -(POSITION.price * 100 * POSITION.shares) * 0.5

            if POSITION.strategy == 'SPY PUT CREDIT SPREAD':
                if POSITION.side == 'SLD':
                    expected_return_postion += (POSITION.price * 100 * POSITION.shares) * 0.5
                    maximum_loss += (POSITION.price * 100 * POSITION.shares) * 0.7
                if POSITION.side == 'BOT':
                    expected_return_postion += -(POSITION.price * 100 * POSITION.shares) * 0.5
                    maximum_loss += -(POSITION.price * 100 * POSITION.shares) * 0.7

            if POSITION.strategy == 'VIX SHORT PUT':
                if POSITION.side == 'SLD':
                    expected_return_postion += (POSITION.price * 100 * POSITION.shares) * 0.5
                    # maximum_loss += (POSITION.price * 100 * POSITION.shares) * 0.7
                if POSITION.side == 'BOT':
                    expected_return_postion += -(POSITION.price * 100 * POSITION.shares) * 0.5
                    # maximum_loss += -(POSITION.price * 100 * POSITION.shares) * 0.7

            if POSITION.strategy == 'VIX CALL BACKSPREAD HEDGE':
                if POSITION.side == 'SLD':
                    expected_return_postion += 0
                    # maximum_loss += (POSITION.price * 100 * POSITION.shares) * 0.7
                if POSITION.side == 'BOT':
                    expected_return_postion += 0
                    # maximum_loss += -(POSITION.price * 100 * POSITION.shares) * 0.7

        expected_return_list += [abs(expected_return_postion)] * len(LOG_POSITIONS)
        maximum_loss_list += [-abs(maximum_loss)] * len(LOG_POSITIONS)

    print(expected_return_list)

    print(len(expected_return_list))
    print(len(MAIN_LOG_FILE))

    MAIN_LOG_FILE['expected_return'] = expected_return_list
    OPEN_POSITIONS['expected_return'] = expected_return_list
    OPEN_POSITIONS_pkl['expected_return'] = expected_return_list

    MAIN_LOG_FILE['maximum_loss'] = maximum_loss_list
    OPEN_POSITIONS['maximum_loss'] = maximum_loss_list
    OPEN_POSITIONS_pkl['maximum_loss'] = maximum_loss_list

    MAIN_LOG_FILE.to_csv('LOGING_FILES/LOG_FILE.csv', index=False)
    MAIN_LOG_FILE.to_pickle('LOGING_FILES/LOG_FILE_backup.pkl')
    OPEN_POSITIONS.to_csv('LOGING_FILES/OPEN_POSITIONS.csv', index=False)
    OPEN_POSITIONS_pkl.to_pickle('LOGING_FILES/OPEN_POSITIONS.pkl')

    # return logg_df


def logging_open(trade, contract_to_pkl, order, strategy, hard_position, ib, condition, time_to_exp):
    for i in range(len(trade.fills)):

        execution = pd.DataFrame([util.dataclassAsDict(trade.fills[i].execution)]).drop(['execId'], axis=1)
        contract = pd.DataFrame([util.dataclassAsDict(trade.fills[i].contract)]).drop(['exchange', 'currency'],
                                                                                      axis=1)  # , ,
        commission = pd.DataFrame([util.dataclassAsDict(trade.fills[i].commissionReport)])  #
        time_trade = trade.fills[i].time
        whatif = ib.whatIfOrder(contract_to_pkl, order)
        state = pd.DataFrame([util.dataclassAsDict(whatif)])
        state = state[
            ['initMarginBefore', 'maintMarginBefore', 'equityWithLoanBefore', 'initMarginChange', 'maintMarginChange',
             'equityWithLoanChange', 'initMarginAfter', 'maintMarginAfter', 'equityWithLoanAfter']]

        execution['time'] = execution['time'].dt.tz_convert(tz='America/New_York').dt.tz_localize(None)
        execution['contract'] = contract_to_pkl
        execution['strategy'] = strategy
        execution['hard_position'] = hard_position
        execution['status'] = 'OPEN'
        execution['PNL_position'] = np.nan
        execution['expected_return'] = np.nan
        execution['condition'] = condition
        execution['date_to_close'] = time_to_exp


        # execution['hard_position'] = hard_position

        logg_df = pd.concat([execution, contract, commission, state], axis=1)
        logg_df = logg_df.drop(
            ['acctNumber', 'permId', 'clientId', 'orderId', 'liquidation', 'orderId', 'liquidation', 'cumQty',
             'avgPrice', 'orderRef', 'evRule', 'evMultiplier', 'modelCode', 'lastLiquidity',
             'secType', 'primaryExchange', 'tradingClass', 'includeExpired', 'secIdType', 'secId',
             'description', 'issuerId', 'comboLegsDescrip', 'comboLegs', 'deltaNeutralContract', 'execId', 'yield_',
             'yieldRedemptionDate', 'equityWithLoanBefore', 'equityWithLoanChange', 'equityWithLoanAfter'], axis=1)

        logg_df = logg_df[['hard_position', 'time', 'symbol', 'exchange', 'strategy', 'condition', 'side', 'shares',
                           'price', 'status', 'PNL_position', 'expected_return', 'contract',
                           'lastTradeDateOrContractMonth', 'date_to_close', 'strike', 'right', 'multiplier', 'localSymbol', 'commission',
                           'currency', 'realizedPNL', 'initMarginBefore',	'maintMarginBefore', 'initMarginChange',
                           'maintMarginChange', 'initMarginAfter', 'maintMarginAfter', 'conId']]

        print('logg_df')
        print(logg_df)

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

        MAIN_LOG_FILE = pd.concat([logg_df, MAIN_LOG_FILE], axis=0, ignore_index=True).reset_index(drop=True)
        OPEN_POSITIONS = pd.concat([logg_df, OPEN_POSITIONS], axis=0, ignore_index=True).reset_index(drop=True)
        OPEN_POSITIONS_pkl = pd.concat([logg_df, OPEN_POSITIONS_pkl], axis=0, ignore_index=True).reset_index(drop=True)

        MAIN_LOG_FILE.to_csv('LOGING_FILES/LOG_FILE.csv', index=False)
        MAIN_LOG_FILE.to_pickle('LOGING_FILES/LOG_FILE_backup.pkl')
        OPEN_POSITIONS.to_csv('LOGING_FILES/OPEN_POSITIONS.csv', index=False)
        OPEN_POSITIONS_pkl.to_pickle('LOGING_FILES/OPEN_POSITIONS.pkl')

    return logg_df


def logging_close(trade, contract_to_buy, order, strategy, conId, hard_position, ib, postition_pnl):
    print('logging_close')
    for i in range(len(trade.fills)):
        execution = pd.DataFrame([util.dataclassAsDict(trade.fills[i].execution)]).drop(['execId'], axis=1)
        contract = pd.DataFrame([util.dataclassAsDict(trade.fills[i].contract)]).drop(['exchange', 'currency'],
                                                                                      axis=1)  # , ,
        commission = pd.DataFrame([util.dataclassAsDict(trade.fills[i].commissionReport)])  #
        time_trade = trade.fills[i].time
        whatif = ib.whatIfOrder(contract_to_buy, order)
        state = pd.DataFrame([util.dataclassAsDict(whatif)])
        state = state[
            ['initMarginBefore', 'maintMarginBefore', 'equityWithLoanBefore', 'initMarginChange', 'maintMarginChange',
             'equityWithLoanChange', 'initMarginAfter', 'maintMarginAfter', 'equityWithLoanAfter']]

        execution['time'] = execution['time'].dt.tz_convert(tz='America/New_York').dt.tz_localize(None)
        execution['contract'] = contract_to_buy
        execution['strategy'] = strategy
        execution['hard_position'] = hard_position
        execution['status'] = 'CLOSE'
        execution['PNL_position'] = postition_pnl - float(trade.fills[i].commissionReport.commission)
        execution['condition'] = np.nan
        execution['expected_return'] = np.nan
        execution['date_to_close'] = np.nan


        logg_df = pd.concat([execution, contract, commission, state], axis=1)
        logg_df = logg_df.drop(
            ['acctNumber', 'permId', 'clientId', 'orderId', 'liquidation', 'orderId', 'liquidation', 'cumQty',
             'avgPrice', 'orderRef', 'evRule', 'evMultiplier', 'modelCode', 'lastLiquidity',
             'secType', 'primaryExchange', 'tradingClass', 'includeExpired', 'secIdType', 'secId',
             'description', 'issuerId', 'comboLegsDescrip', 'comboLegs', 'deltaNeutralContract', 'execId', 'yield_',
             'yieldRedemptionDate', 'equityWithLoanBefore', 'equityWithLoanChange', 'equityWithLoanAfter'], axis=1)

        logg_df = logg_df[['hard_position', 'time', 'symbol', 'exchange', 'strategy', 'side', 'shares', 'price', 'status',
                           'PNL_position', 'expected_return', 'contract', 'condition', 'lastTradeDateOrContractMonth',
                           'strike', 'right', 'multiplier', 'localSymbol', 'commission', 'currency', 'realizedPNL',
                           'initMarginBefore',	'maintMarginBefore', 'initMarginChange', 'maintMarginChange',
                           'initMarginAfter', 'date_to_close', 'maintMarginAfter', 'conId']]

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

        # удаляем закрытую позицию из лога открытых позиций
        OPEN_POSITIONS = OPEN_POSITIONS[OPEN_POSITIONS['conId'] != conId]
        OPEN_POSITIONS_pkl = OPEN_POSITIONS_pkl[OPEN_POSITIONS_pkl['conId'] != conId]
        # логируем закрытие в основной лог файл
        MAIN_LOG_FILE = pd.concat([logg_df, MAIN_LOG_FILE], axis=0, ignore_index=True).reset_index(drop=True)

        MAIN_LOG_FILE.to_csv('LOGING_FILES/LOG_FILE.csv', index=False)
        OPEN_POSITIONS.to_csv('LOGING_FILES/OPEN_POSITIONS.csv', index=False)
        OPEN_POSITIONS_pkl.to_pickle('LOGING_FILES/OPEN_POSITIONS.pkl')

    return logg_df

    # MAIN_LOG_FILE['expected_return'] = expected_return_list
    # OPEN_POSITIONS['expected_return'] = expected_return_list
    # OPEN_POSITIONS_pkl['expected_return'] = expected_return_list
    #
    # try:
    #     MAIN_LOG_FILE = pd.read_csv('LOGING_FILES/LOG_FILE.csv').reset_index(drop=True)
    #     MAIN_LOG_FILE.to_csv('LOGING_FILES/LOG_FILE_backup.csv', index=False)
    #     OPEN_POSITIONS = pd.read_csv('LOGING_FILES/OPEN_POSITIONS.csv').reset_index(drop=True)
    #     OPEN_POSITIONS.to_csv('LOGING_FILES/OPEN_POSITIONS_backup.csv', index=False)
    #     OPEN_POSITIONS_pkl = pd.read_pickle('LOGING_FILES/OPEN_POSITIONS.pkl').reset_index(drop=True)
    # except:
    #     MAIN_LOG_FILE = pd.DataFrame()
    #     OPEN_POSITIONS = pd.DataFrame()
    #     OPEN_POSITIONS_pkl = pd.DataFrame()
    #
    #
    # MAIN_LOG_FILE.to_csv('LOGING_FILES/LOG_FILE.csv', index=False)
    # OPEN_POSITIONS.to_csv('LOGING_FILES/OPEN_POSITIONS.csv', index=False)
    # OPEN_POSITIONS_pkl.to_pickle('LOGING_FILES/OPEN_POSITIONS.pkl')
