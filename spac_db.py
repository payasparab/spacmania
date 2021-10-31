import requests
import pandas as pd
import os
import selenium





class SPAC_DB:
    def __init__(self):
        
        ''' 
        Pull in raw data that makes up
        '''
        self.raw_data_root = os.path.dirname(os.path.abspath(__file__)) + '\\data\\'
        self.unmerged = pd.read_csv(self.raw_data_root + 'spac_data\\unannounced_merger_spacs.csv')
        self.pending = pd.read_csv(self.raw_data_root + 'spac_data\\pending_merger_spacs.csv')
        self.public_spacs = pd.read_csv(self.raw_data_root + 'spac_data\\public_merged_spacs.csv')
        self.price_data = pd.read_csv(self.raw_data_root + 'price_data.csv').set_index(['date', 'ticker']).sort_index()
        
    def update_csvs(self):
        '''
        Method that pulls web pages links and updates csv files in the spac_data root
        '''
        unpending_merger_link = 'https://stockmarketmba.com/listofspacswithoutapendingmerger.php'
        pending_merger_link = 'https://stockmarketmba.com/pendingspacmergers.php'
        publically_listed_spacs = 'https://stockmarketmba.com/listofcompaniesthathavemergedwithaspac.php'

        
if __name__ == '__main__':
    sdb = SPAC_DB()

