##*****************
# Operations to manage trading personas 
##*****************

import sqlite3
from datetime import datetime

#create a python function that updates a user in a database
def update_persona(persona_name, description):
    #optimize by passing the c variable into the function and initialize the db once
    con = sqlite3.connect('portfolio.db')
    c = con.cursor()

    c.execute("UPDATE persona SET description = ? WHERE persona_name = ?", (persona_name, description))
    msg = c.rowcount + " row was updated."
    return msg
  
def delete_persona(persona_name):
    #optimize by passing the c variable into the function and initialize the db once
    con = sqlite3.connect('portfolio.db')
    c = con.cursor()

    c.execute("DELETE FROM persona WHERE persona_name = ?", (persona_name))

    msg = c.rowcount + " row was deleted."
    return msg

#create a python function that will return a dictionary object of a user from a database
def get_persona(persona_name):

    con = sqlite3.connect('portfolio.db')
    c = con.cursor()

    # query the database for the user with the given id
    persona = c.query('SELECT * FROM users WHERE persona_name = ?', [persona_name])

    # create a dictionary object of the user
    persona_dict = {
        'persona_name': persona['persona_name'],
        'description': persona['description']
    }

    return persona_dict

def buy_stock(ticker,buy_price, persona_name):

    con = sqlite3.connect('portfolio.db')
    c = con.cursor()

    buy_date = datetime.now()

    # insert new buy transaction into the table
    sql = "INSERT INTO trade_history (buy_date, ticker, buy_price, persona_name) VALUES (%s, %s, %s, %s)"
    val = (buy_date, ticker, buy_price, persona_name)
    c.execute(sql,val)

    msg = c.rowcount + " row was inserted."
    return msg

def sell_stock(ticker,sell_price, persona_name, buy_price):

    con = sqlite3.connect('portfolio.db')
    c = con.cursor()

    sell_date = datetime.now()

    # insert new buy transaction into the table
    c.execute("UPDATE trade_history SET sell_price = ? AND sell_date = ? WHERE persona_name = ? AND ticker = ? AND buy_price = ?", (sell_price,sell_date,persona_name,ticker,buy_price))
    
    msg = c.rowcount + " row was updated."
    return msg

def get_current_portfolio(persona_name):

    con = sqlite3.connect('portfolio.db')
    c = con.cursor()

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

