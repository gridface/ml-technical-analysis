#use python fastapi to create an api set to perform crud actions against a database
from fastapi import FastAPI, Query
import sqlite3
import trader

# Exposed API methods to perform crud actions on the following tables:
#   stocks 
#   persona 
#   trade_history

con = sqlite3.connect('portfolio.db')
c = con.cursor()

app = FastAPI()
   
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