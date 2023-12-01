##*****************
# DB Operations to manage trading personas and personal portfolios
##*****************

from datetime import datetime

#create a python function that updates a user in a database
def create_persona(c, persona_name, description):

    # insert new buy transaction into the table
    sql = "INSERT INTO persona (persona_name, description) VALUES (%s, %s)"
    val = (persona_name, description)
    c.execute(sql,val)

    msg = c.rowcount + " row was inserted."
    return msg

#create a python function that updates a user in a database
def update_persona(c, persona_name, description):

    c.execute("UPDATE persona SET description = ? WHERE persona_name = ?", (persona_name, description))
    msg = c.rowcount + " row was updated."
    return msg
  
def delete_persona(c, persona_name):

    c.execute("DELETE FROM persona WHERE persona_name = ?", (persona_name))

    msg = c.rowcount + " row was deleted."
    return msg

#create a python function that will return a dictionary object of a user from a database
def get_persona(c, persona_name):

    # query the database for the user with the given id
    persona = c.query('SELECT * FROM persona WHERE persona_name = ?', [persona_name])

    # create a dictionary object of the user
    persona_dict = {
        'persona_name': persona['persona_name'],
        'description': persona['description']
    }

    return persona_dict

#create a python function that will return a dictionary object of a user from a database
def get_all_personas(c):

    # query the database for the user with the given id
    persona = c.execute('SELECT * FROM persona')

    # create a dictionary object of the user
    persona_dict = {
        'persona_name': persona['persona_name'],
        'description': persona['description']
    }

    return persona_dict

def buy_stock(c,ticker,buy_price, persona_name):

    buy_date = datetime.now()

    # insert new buy transaction into the table
    sql = "INSERT INTO trade_history (buy_date, ticker, buy_price, persona_name) VALUES (%s, %s, %s, %s)"
    val = (buy_date, ticker, buy_price, persona_name)
    c.execute(sql,val)

    msg = c.rowcount + " row was inserted."
    return msg

def sell_stock(c, ticker,sell_price, persona_name, buy_price):

    sell_date = datetime.now()

    # insert new buy transaction into the table
    c.execute("UPDATE trade_history SET sell_price = ? AND sell_date = ? WHERE persona_name = ? AND ticker = ? AND buy_price = ?", (sell_price,sell_date,persona_name,ticker,buy_price))
    
    msg = c.rowcount + " row was updated."
    return msg

def get_current_portfolio(c, persona_name):

    # query the database for the user with the given id
    trade_history = c.query('SELECT * FROM trade_history WHERE persona_name = ?', [persona_name])

    # create a dictionary object of the user
    trade_history_dict = {
        'persona_name': trade_history['persona_name'],
        'ticker': trade_history['description'],
        'buy_date': trade_history['persona_name'],
        'buy_price': trade_history['description'],
        'sell_date': trade_history['persona_name'],
        'sell_price': trade_history['description']
    }
    return trade_history_dict

