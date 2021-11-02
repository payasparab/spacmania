from pandas import tseries
import requests
import pandas as pd
import numpy as np
import os
import re 

from data_utils import df_cleaner
from data_utils import str_num_cleaner

class SPAC_DB:
    def __init__(self, volume_windows=[5, 21, 63]):
        
        ''' 
        Pull in raw data that makes up the key components of the database
        '''
        self.raw_data_root = os.path.dirname(os.path.abspath(__file__)) + '\\data\\'
        self.volume_windows = volume_windows

        # Pull in unmerged SPAC data - just financial institution #
        self.unmerged = pd.read_csv(self.raw_data_root + 'spac_data\\unannounced_merger_spacs.csv')
        self.unmerged = df_cleaner(self.unmerged)
        self.unmerged['ipo_date'] = pd.to_datetime(self.unmerged.ipo_date)    
        self.unmerged['market_cap'] = self.unmerged.market_cap.apply(str_num_cleaner)
        self.unmerged['shares_outstanding'] = self.unmerged.shares_outstanding.apply(str_num_cleaner)
        
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

        date, company = parse_proposed_merger(self.pending['proposed_merger'])
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
        self.public_spacs['ipo_date'] = pd.to_datetime(self.public_spacs.ipo_date)
        self.public_spacs['market_cap'] = self.public_spacs.market_cap.apply(
            str_num_cleaner
        )
        self.public_spacs['shares_outstanding'] = self.public_spacs.shares_outstanding.apply(
            str_num_cleaner
        )
        self.public_spacs['average_trading_volume'] = self.public_spacs.average_trading_volume.apply(
            str_num_cleaner
        )
        
        # Pull and Process Price Data #
        self.price_data = pd.read_csv(self.raw_data_root + 'price_data.csv')
        self.price_data['date'] = pd.to_datetime(self.price_data.date)
        self.price_data = self.price_data.set_index(['date', 'ticker']).sort_index()
        self.price_data['rets'] = self.price_data.unstack().pct_change(fill_method=None).stack()

        # Save Key Metadata from Price Data #
        self.all_tickers = list(self.price_data.index.get_level_values('ticker').unique())
        
        # Save where the Adjusted Price Data Starts
        tseries_start = {}
        for _tick in self.all_tickers:
            _start = self.price_data.xs(_tick, level='ticker').index.min()
            tseries_start[_tick] = _start
        self.tseries_start = pd.DataFrame(
            index = tseries_start.keys(), 
            data = tseries_start.values(), 
            columns = ['start_date']
        )
        self.tseries_start.index.name = 'ticker'

        
        self.volume_data = pd.read_csv(self.raw_data_root + 'volume_data.csv')
        self.volume_data['date'] = pd.to_datetime(self.volume_data.date)
        self.volume_data = self.volume_data.set_index(['date', 'ticker']).sort_index()
        for vw in self.volume_windows:
            _name = '{}_ewm_volume'.format(vw)
            _series = self.volume_data.volume.unstack().ewm(span=vw, min_periods=np.round(vw/3)).mean().stack()
            self.volume_data[_name] = _series
    

        # Key metadata
       

    def create_master_db(self):
        '''
        Code to combine the preloaded SPAC information and price information to create
        a time series indexed by date/ticker and key information regarding. 

        Contains: 
            > date
            > ticker
            > price
            > 1-day returns
            > trading volume

            > industry 
            > ipo_date
        '''
        # Combine volume and price data to map to 3 datasets
        price_volume = self.price_data.join(self.volume_data).reset_index()

        # Combine unmerged #
        um = self.unmerged[['name', 'symbol', 'ipo_book_value', 'shares_outstanding', 'ipo_date']]
        um = um.rename(columns={'symbol': 'ticker'})
        um = price_volume.merge(um, on='ticker')
        um['price_to_book'] = um.adjClose / um.ipo_book_value
        um['market_cap'] = um.adjClose * um.shares_outstanding
        um['days_since_ipo'] = (um.date - um.ipo_date).dt.days
        um = um[(um.days_since_ipo >= 0)] # Filter stocks that would not have been tradable
        um = um.set_index(['date', 'ticker']).sort_index()

        # Combine Pending Merger #
        pend = self.pending[[
            'symbol', 'name', 'shares_outstanding', 
            'merger_proposed_date', 
        ]]
        pend = pend.rename(columns={'symbol': 'ticker'})
        pend = price_volume.merge(pend, on='ticker')
        pend = pend.merge(self.tseries_start, on='ticker')
        pend['days_since_start'] = (pend.date - pend.start_date).dt.days
        _merge_announce = (pend.date - pend.merger_proposed_date).dt.days
        _merge_announce[_merge_announce < 0] = 0 #adjustment to prevent lookahead bias
        pend['days_since_merger_announced'] = _merge_announce 

        self.master_db = master_db


    def update_csvs(self):
        '''
        Method that pulls web pages links and updates csv files in the spac_data root
        '''
        #TODO: Not super urgent, but necessary for end-to-end
        unpending_merger_link = 'https://stockmarketmba.com/listofspacswithoutapendingmerger.php'
        pending_merger_link = 'https://stockmarketmba.com/pendingspacmergers.php'
        publically_listed_spacs = 'https://stockmarketmba.com/listofcompaniesthathavemergedwithaspac.php'

        
if __name__ == '__main__':
    sdb = SPAC_DB()

