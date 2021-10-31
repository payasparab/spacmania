import requests
import pandas as pd
import os
import re 

from data_utils import pd_name_cleaner

class SPAC_DB:
    def __init__(self):
        
        ''' 
        Pull in raw data that makes up
        '''
        self.raw_data_root = os.path.dirname(os.path.abspath(__file__)) + '\\data\\'
        self.unmerged = pd.read_csv(self.raw_data_root + 'spac_data\\unannounced_merger_spacs.csv')
        self.unmerged.columns = list(self.unmerged.columns).apply(pd_name_cleaner)
        
        self.pending = pd.read_csv(self.raw_data_root + 'spac_data\\pending_merger_spacs.csv')
        self.pending['']
        
        merger_proposal_data = 1 #\d+\/\d+\/\d+




        self.public_spacs = pd.read_csv(self.raw_data_root + 'spac_data\\public_merged_spacs.csv')
        
        
        self.price_data = pd.read_csv(self.raw_data_root + 'price_data.csv').set_index(['date', 'ticker']).sort_index()
        self.price_data['rets'] = self.price_data.unstack().pct_change(fill_method=None).stack()


    def update_csvs(self):
        '''
        Method that pulls web pages links and updates csv files in the spac_data root
        '''
        unpending_merger_link = 'https://stockmarketmba.com/listofspacswithoutapendingmerger.php'
        pending_merger_link = 'https://stockmarketmba.com/pendingspacmergers.php'
        publically_listed_spacs = 'https://stockmarketmba.com/listofcompaniesthathavemergedwithaspac.php'

        
if __name__ == '__main__':
    sdb = SPAC_DB()

