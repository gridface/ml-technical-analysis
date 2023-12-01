import pandas as pd, yfinance as yf, sqlite3, libs.collector as collector

#*******************
#Operations centered around initializing db and loading stocks into a sqlite database
#*******************

def initialize_db():
    con = sqlite3.connect('portfolio.db')
    c = con.cursor()

        
    #create stocks table
    create_stocks_table = """CREATE TABLE IF NOT EXISTS stocks (
        date TEXT NOT NULL,
        ticker TEXT NOT NULL,
        open REAL,
        close REAL,
        volume REAL
        )"""

    #create stock info table
    create_stock_info_table = """CREATE TABLE IF NOT EXISTS stock_info (
        ticker TEXT NOT NULL,
        name TEXT NOT NULL,
        industry TEXT,
        subindustry TEXT
        )"""  


    #create company table - this has to be created first because persona uses
    #company id as a foreign key
    create_company_table = """CREATE TABLE IF NOT EXISTS company (
        id INTEGER NOT NULL PRIMARY KEY,
        company_name TEXT NOT NULL,
        industry TEXT,
        subindustry TEXT
        )"""  
    #create persona table
    create_persona_table = """CREATE TABLE IF NOT EXISTS persona (
        id INTEGER NOT NULL PRIMARY KEY,
        persona_name TEXT NOT NULL,
        description TEXT,
        company_id TEXT,
        FOREIGN KEY (company_id)
            REFERENCES company (id) 
        )"""    

    #create trade_history table
    create_trade_history = """CREATE TABLE IF NOT EXISTS trade_history (
        id INTEGER NOT NULL PRIMARY KEY,
        buy_date TEXT NOT NULL,
        ticker TEXT NOT NULL,
        persona_name TEXT NOT NULL,
        sell_date TEXT,
        buy_price REAL,
        sell_price REAL
        )"""     

    #a view can be created separately for company portfolio


    c.execute(create_stocks_table.replace('\n',' '))
    c.execute(create_stock_info_table.replace('\n',' '))
    c.execute(create_persona_table.replace('\n',' '))
    c.execute(create_company_table.replace('\n',' '))
    c.execute(create_trade_history.replace('\n',' '))

    return("db initialized and tables created")


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

    con = sqlite3.connect('portfolio.db')
    c = con.cursor()

    # construct query
    drop_stocks = """
    DROP TABLE stocks
    """
    drop_stock_info = """
    DROP TABLE stock_info
    """
    drop_persona = """
    DROP TABLE persona
    """
    drop_company = """
    DROP TABLE company
    """
    drop_trade_history = """
    DROP TABLE trade_history
    """
    c.execute(drop_stocks.replace('\n',' '))
    c.execute(drop_stock_info.replace('\n',' '))
    c.execute(drop_persona.replace('\n',' '))
    c.execute(drop_company.replace('\n',' '))
    c.execute(drop_trade_history.replace('\n',' '))

    return("dropped database tables")

#write a python function using pandas that loads data into a table called persona from a csv file on my hard drive
def load_company_data():
    con = sqlite3.connect('portfolio.db')
    file_path = '/Users/acagle/dev/datasci/ml-technical-analysis/libs/company.csv'
    df = pd.read_csv(file_path)
    df.to_sql('company', con, if_exists='replace')
    return("loaded company data")

def load_persona_data():
    con = sqlite3.connect('portfolio.db')
    file_path = '/Users/acagle/dev/datasci/ml-technical-analysis/libs/persona.csv'
    df = pd.read_csv(file_path)
    df.to_sql('persona', con, if_exists='replace')
    return("loaded persona data")

def load_stock_info_data():
    con = sqlite3.connect('portfolio.db')
    table=pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    df = table[0]
    stock_info = df.iloc[:,:4]
    stock_info.rename(columns={'Symbol': 'ticker', 'Security': 'name', 'GICS Sector': 'industry', 'GICS Sub-Industry': 'subindustry'}, inplace=True)
    stock_info.to_sql('stock_info', con, if_exists='replace')
    return("loaded stock info data")