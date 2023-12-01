#use python fastapi to create an api set to perform crud actions against a database
from fastapi import FastAPI, Query
import sqlite3
import libs.trader as trader
import libs.dbloader as dbloader

# Exposed API methods to perform crud actions on the following tables:
#   stocks 
#   persona 
#   trade_history

con = sqlite3.connect('portfolio.db')
c = con.cursor()

app = FastAPI()

@app.get("/")
async def root():
    return {"message": "an api for executing paper trades."}

@app.post("/new_persona")    
def create_record(persona_name: str, description: str = Query(None)):
    trader.create_persona(c, persona_name, description)
    return {"message": "Record created successfully"}

@app.get("/get_persona/{persona_name}") 
async def get_persona(persona_name: str):   
    record = await trader.get_persona(c, persona_name)
    return {record}  

@app.get("/get_personas") 
async def get_persona():   
    record = await trader.get_all_personas(c)
    return {record}  

@app.put("/update_persona")
def update_record(persona_name: str, description: str = Query(None)):
    trader.update_persona(c, persona_name, description)
    return {"message": "Record updated successfully"}

@app.post("/load_company_table")
def load_company_table():
    dbloader.load_company_data()
    return {"message": "table updated successfully"}
#write a curl command that calls load_db_table and passes two query params
#write a curl command that passes a file path as a query param
# curl -X POST http://127.0.0.1:8000/load_db_table?table_name=company&file_path=./company.csv

@app.post("/load_persona_table")
def load_persona_table():
    dbloader.load_persona_data()
    return {"message": "table updated successfully"}

@app.post("/load_stock_info_data")
def load_stock_info_data():
    dbloader.load_stock_info_data()
    return {"message": "table updated successfully"}

@app.post("/initialize_db")
def init_db():
    dbloader.initialize_db()
    return {"message": "db initialized"}
#create a curl command that will call the update_record api.
# curl -X POST http://127.0.0.1:8000/initialize_db 

@app.post("/drop_tables")
def init_db():
    dbloader.drop_tables()
    return {"message": "db tables dropped"}

@app.post("/db_setup")
def db_setup():
    dbloader.drop_tables()
    dbloader.initialize_db()
    dbloader.load_company_data()
    dbloader.load_persona_data()
    dbloader.load_stock_info_data()
    return {"message": "db tables setup"}



#create a curl command that will call the update_record api.
# curl -X POST http://127.0.0.1:8000/initialize_db 
# curl -X POST \
#   http://127.0.0.1:8000/initialize_db \
#   -H 'Content-Type: application/json' \
#   -d '{ "id": "12345", "name": "John Doe" }'


#create a python function that will take command line inputs of table_name and file_name to load a sql table from a csv file using pandas. now create python code that will let me call that function from the command line
