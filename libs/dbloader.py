import pandas as pd, yfinance as yf, sqlite3, collector

#*******************
#Operations centered around loading data into a sqlite database
#*******************

def initialize_db():
    con = sqlite3.connect('portfolio.db')
    c = con.cursor()

        
    #create stocks table
    create_stocks_table = """CREATE TABLE IF NOT EXISTS stocks (
        Date TEXT NOT NULL,
        ticker TEXT NOT NULL,
        open REAL,
        close REAL,
        volume, REAL
        PRIMARY KEY(Date, ticker)
        )"""

    #create persona table
    create_persona_table = """CREATE TABLE IF NOT EXISTS persona (
        persona_name TEXT NOT NULL,
        description TEXT,
        PRIMARY KEY(persona_name)
        )"""    

    #create trade_history table
    create_trade_history = """CREATE TABLE IF NOT EXISTS trade_history (
        buy_date TEXT NOT NULL,
        ticker TEXT NOT NULL,
        persona_name TEXT NOT NULL,
        sell_date TEXT,
        buy_price REAL,
        sell_price REAL,
        PRIMARY KEY(persona_name, ticker,buy_date)
        )"""    


    c.execute(create_stocks_table.replace('\n',' '))
    c.execute(create_persona_table.replace('\n',' '))
    c.execute(create_trade_history.replace('\n',' '))


def load_stocks(stocks: list, period: str = "1y"):

    stocks = collector.get_yfinance_stocks(stocks,period)


def get_prices():
    # construct query
    select_query = """
    select * from stocks
    """
    c.execute(select_query.replace('\n',' '))
    
    result = pd.DataFrame(c.fetchall(), columns = ['Date', 'ticker', 'close'])
    # convert to datetime
    result['Date'] = pd.to_datetime(result['Date'])

    return result

def drop_tables():
    # construct query
    drop_stocks = """
    DROP TABLE stocks
    """

    drop_persona = """
    DROP TABLE persona
    """

    drop_trade_history = """
    DROP TABLE trade_history
    """
    c.execute(drop_stocks.replace('\n',' '))
    c.execute(drop_persona.replace('\n',' '))
    c.execute(drop_trade_history.replace('\n',' '))

    return