import pandas as pd, yfinance as yf, sqlite3, collector

#*******************
#Operations centered around loading data into a sqlite database
#*******************

def load_db(portfolio: list, period: str = "1y"):

#set up database
    con = sqlite3.connect('portfolio.db')
    c = con.cursor()

    #create price table
    create_price_table_query = """CREATE TABLE IF NOT EXISTS prices (
        Date TEXT NOT NULL,
        ticker TEXT NOT NULL,
        price REAL,
        PRIMARY KEY(Date, ticker)
        )"""
    
    #create volume table
    create_volume_table_query = """CREATE TABLE IF NOT EXISTS volume(
        Date TEXT NOT NULL,
        ticker TEXT NOT NULL,
        volume REAL,
        PRIMARY KEY(Date, ticker)
        )"""

    c.execute(create_price_table_query.replace('\n',' '))
    c.execute(create_volume_table_query.replace('\n',' '))

    stocks = collector.get_yfinance_stocks(portfolio,period)
    
    adj_close = stocks['Adj Close']
    volume = stocks['Volume']

    # convert wide to long
    adj_close_long = pd.melt(adj_close.reset_index(), id_vars='Date', value_vars=portfolio, var_name ="ticker", value_name="price")
    volume_long = pd.melt(volume.reset_index(), id_vars='Date', value_vars=portfolio, var_name = "ticker", value_name = "volume")

    adj_close_long.to_sql('prices', con, if_exists='append', index=False)
    volume_long.to_sql('volume', con, if_exists='append', index=False)


def get_prices():
    # construct query
    select_query = """
    select * from prices
    """
    c.execute(select_query.replace('\n',' '))
    
    result = pd.DataFrame(c.fetchall(), columns = ['Date', 'ticker', 'price'])
    # convert to datetime
    result['Date'] = pd.to_datetime(result['Date'])

    return result

def drop_tables():
    # construct query
    drop_query1 = """
    DROP TABLE prices
    """

    drop_query2 = """
    DROP TABLE prices
    """
    c.execute(drop_query1.replace('\n',' '))
    c.execute(drop_query2.replace('\n',' '))
    return