'''
Data Standardization Tools for Consistency
'''

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
    