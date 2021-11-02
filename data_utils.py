'''
Data Standardization Tools for Consistency
'''

from datetime import date


def df_cleaner(df):
    

    def pd_name_cleaner(ugly_str):
        ''' Make any string pandas friendly '''
        import re
        ugly_str = ugly_str.lower() # Lowercase 
        ugly_str = ugly_str.replace(' ', '_') # No spaces
        ugly_str = re.sub(r'\W+', '', ugly_str) # Alpha numeric only
        ugly_str = ugly_str[:100] # Shorten
        return ugly_str


    df.columns = list(map(pd_name_cleaner, list(df.columns)))

    return df 


def str_num_cleaner(num):
    num = num.replace('$', '')
    num = num.replace(',', '')
    return float(num)


def calc_momentum(df, window, risk_adjusted=True):
    '''
    Args: 
        - df : pd.DF :
            > index : Multiindex : ('date', 'ticker')
            > columns : Series : rets : returns from equity
        - window : int : window to calculate back momentum
        - risk_adjusted : bool : 
            > True->Use Rolling Sharpe ratio
            > False-> Use Excess returns 
    '''
    if risk_adjusted: 
        pass
    else: 
        pass
        