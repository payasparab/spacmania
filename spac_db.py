import requests
import pandas as pd
import numpy as np
import os
import re 

from data_utils import df_cleaner
from data_utils import str_num_cleaner

class SPAC_DB:
    def __init__(self):
        
        ''' 
        Pull in raw data that makes up the key components of the database
        '''
        self.raw_data_root = os.path.dirname(os.path.abspath(__file__)) + '\\data\\'
        
        # Pull in unmerged SPAC data - just financial institution #
        self.unmerged = pd.read_csv(self.raw_data_root + 'spac_data\\unannounced_merger_spacs.csv')
        self.unmerged = df_cleaner(self.unmerged)
        self.unmerged['ipo_date'] = pd.to_datetime(self.unmerged.ipo_date)    
        self.unmerged['market_cap'] = self.unmerged.market_cap.apply(str_num_cleaner)
        self.unmerged['shares_outstanding'] = self.unmerged.shares_outstanding.apply(str_num_cleaner)
        self.unmerged.drop(columns=[''], inplace=True)
        
        self.pending = pd.read_csv(self.raw_data_root + 'spac_data\\pending_merger_spacs.csv')
        self.pending = df_cleaner(self.pending)
        
        
        def parse_proposed_merger(proposed_merger_col):
            '''
            Helper function to extract key details for the proposed merger using regex.

            Returns tuple of pd.Series (date, company) 
            '''
            def process_date(merger_desc):
                '''
                Find the date in the merger description.
                '''
                date_matches = re.findall(
                    r"On \d+\/\d+\/\d+", merger_desc
                )
                if date_matches:
                    return date_matches[0][3:]
                else: 
                    return np.nan
            
            def process_company(merger_desc):
                comp_matches = re.findall(
                    r"(?=with)(.*)", merger_desc
                )
                if comp_matches:
                    return comp_matches[0][5:].split(',')[0]
                else: 
                    return np.nan
            
            date = pd.to_datetime(
                proposed_merger_col.apply(
                    lambda x: process_date(x)
                )
            )

            company = proposed_merger_col.apply(
                lambda x: process_company(x)
            )

            return date, company

        date, company = parse_proposed_merger(self.pending['poposed_merger'])
        # TODO: Needs manual override + Error handling
        self.pending['merger_proposed_date'] = date
        self.pending['merger_propsed_company'] = company
        self.pending['shares_outstanding'] = self.pending.shares_outstanding.apply(
            str_num_cleaner
        )
        self.pending['average_trading_volume'] = self.pending.average_trading_volume.apply(
            str_num_cleaner
        )
        
        self.public_spacs = pd.read_csv(self.raw_data_root + 'spac_data\\public_merged_spacs.csv')
        self.public_spacs = df_cleaner(self.public_spacs)
        self.public_spacs['ipo_date'] = pd.to_datetime(self.public_spacs)
        self.public_spacs['market_cap'] = pd.to_datetime()
        self.public_spacs['shares_outstanding'] = self.public_spacs.shares_outstanding.apply(
            str_num_cleaner
        )
        self.public_spacs['average_trading_volume'] = self.average_trading_volume.apply(
            str_num_cleaner
        )
        
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

