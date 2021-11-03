from ib_insync.util import df as df_util
from ib_insync import util
from ib_insync import IB
from ib_insync import Contract, Stock, MarketOrder, Future

def stock_ticker_to_contract(tickers):

    tickers = [x  if '.' not in x else x.split('.')[0]+' '+
                                    x.split('.')[1] for x in tickers]
    contracts = [Stock(x, 'SMART','USD') for x in tickers]
    contracts = self.ib.qualifyContracts(*contracts)

    return contracts