import pandas as pd, yfinance as yf, sqlite3

#*******************
# Operations for getting data from finance API
#*******************

#what period do you want to evaluate?
# use "period" instead of start/end
# valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
# (optional, default is '1mo')
per = "1y"


#notice the format of this data - it has a two-tiered header - [measurement]/[ticker]
# we will be changing this around below to make it easier to work with
# portfolio takes in an array of values
def get_yfinance_stocks(portfolio: list, period: str = per):
    stocks = yf.download(portfolio,period=per)
    return stocks

def flatten(Data):

    flat_stocks = Data.copy()
    #this step flattens out the index 
    flat_stocks.columns = flat_stocks.columns.to_flat_index()

    #creates a multiindex for better sorting and grouping of data
    # check this link for more info https://pandas.pydata.org/docs/user_guide/advanced.html
    flat_stocks.columns = pd.MultiIndex.from_tuples(flat_stocks.columns)

    #lets switch levels now so we can group on individual stocks at the top level
    flat_stocks.swaplevel(axis = 1).sort_index(axis=1)

    return flat_stocks





