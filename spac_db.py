import requests
import pandas as pd
import os
import selenium


from ib_insync.util import df as df_util
from ib_insync import util
from ib_insync import IB
from ib_insync import Contract, Stock, MarketOrder, Future



class SPAC_DB:
    def __init__(self):
        
        ''' 
        Pull in raw data that makes up
        '''
        self.raw_data_root = os.path.dirname(os.path.abspath(__file__)) + '\\spac_data\\'
        self.unmerged = pd.read_csv(self.raw_data_root + 'unannounced_merger_spacs.csv')
        self.pending = pd.read_csv(self.raw_data_root + 'pending_merger_spacs.csv')
        self.public_spacs = pd.read_csv(self.raw_data_root + 'public_merged_spacs.csv')

        
    def update_csvs(self):
        '''
        Method that pulls web pages links and updates csv files in the spac_data root
        '''
        unpending_merger_link = 'https://stockmarketmba.com/listofspacswithoutapendingmerger.php'
        pending_merger_link = 'https://stockmarketmba.com/pendingspacmergers.php'
        publically_listed_spacs = 'https://stockmarketmba.com/listofcompaniesthathavemergedwithaspac.php'

        
    def create_returns_series(self): 
        '''
        
        '''