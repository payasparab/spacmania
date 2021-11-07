import pandas as pd
import numpy as np
from spac_db import SPAC_DB

class SPACTradeSimulator:
    '''
    Key Configuration

    pre_spac_allocation : percent of portfolio to unmerged SPACs
    pending_spac_allocation : percent allocated to SPACs announced
    post_spac_allocation : percent allocated to recently merged SPACs
    leverage_ratio : amount of portfolio level leverage 
    rebalance_freq : number of days before rebalance

    ticker_universe_min : number of tickers in SPAC universe before investing

    '''
    def __init__(self):
        self.pre_spac_allocation = 0.2
        self.pending_spac_allocation = 0.4
        self.post_spac_allocation = 0.4

        self.leverage_ratio = 1.0
        self.pre_spac_allocation_lev = self.pre_spac_allocation * self.leverage_ratio
        self.pending_spac_allocation_lev = self.pending_spac_allocation * self.leverage_ratio
        self.post_spac_allocation_lev = self.post_spac_allocation_lev * self.leverage_ratio


        self.rebalance_freq = 5

        self.sdb = SPAC_DB()
        self.ticker_universe_min = 25
        self.start_date = self.sdb.master_db.groupby('date').adjClose.count().where(
            lambda x: x > self.ticker_universe_min
        ).dropna().index.min()
        self.trade_dates = self.sdb.master_db.index.get_level_values('date').unique().where(
            lambda x: x > self.start_date
        )

        self.trade_exec_days = [
            self.trade_dates[i] for i in range(len(self.trade_dates)) if (i%self.rebalance_freq == 0)
        ]
        

            

    def determine_trading_universe(self, trade_date):
        '''
        Find the trading universe between pre, pending, and completed SPACs on trade_date
        Also, pull in historical data for each of these at that time. 

        
        '''
        _universe = self.sdb.master_db(trade_date, level='date')
        

    def f