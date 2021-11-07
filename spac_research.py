import numpy as np
import pandas as pd

from spac_db import SPAC_DB
sdb = SPAC_DB()

def calculate_post_ipo_sharpe(sdb=sdb, max_days=251):
    '''
    This method validates that post ipo, there is anticipated growth
    '''
    
    completed = sdb.key_db['completed']
    tickers = completed.index.get_level_values('ticker').unique()
    post_ipo_sharpes = []
    idxmax_ticker = {}
    sharpemax_ticker = {}
    for _ticker in tickers:
        _completed = completed.xs(_ticker, level='ticker')
        _completed = _completed.query(
            'days_since_ipo >= -2'
        ).query(
            'days_since_ipo <= {}'.format(max_days)
        )
        _vol =_completed.rets.rolling(5, min_periods=2).std() * np.sqrt(251)
        _rets = _completed.total_return
        _sharpe = (_rets/_vol).to_frame()
        _sharpe.columns = [_ticker]
        _sharpe['days_since_ipo'] = _completed.days_since_ipo
        _sharpe = _sharpe.reset_index().set_index('days_since_ipo')
        _sharpe.drop(columns=['date'], inplace=True)
        _sharpe = _sharpe.dropna()
        if len(_sharpe) > 0:
            idxmax_ticker[_ticker] = _sharpe.iloc[2:].idxmax()
            sharpemax_ticker[_ticker] = _sharpe.iloc[2:].max()
            post_ipo_sharpes.append(_sharpe)

    post_ipo_sharpe_table = pd.concat(post_ipo_sharpes, axis=1)
    post_ipo_sharpe_table = post_ipo_sharpe_table.clip(-3,3)
    return post_ipo_sharpe_table.mean(axis=1)


    
    