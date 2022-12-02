from fastapi import FastAPI
from main import data_loader
# from sqlalchemy import create_engine, types, text
import pandas as pd
# import json

app = FastAPI()
database = data_loader()
conn = database.create_connection('planning.db')

@app.get("/populate_data")
async def root():
    database = data_loader()
    conn = database.create_connection('planning.db')
    db = database.create_table('./Schema.sql', conn)
    warehouse = database.format_and_load_json_data('./../planning.json',conn,'planning')
    return {"success": True , "message": "Data have been populated successfully"}

@app.get("/most_listed_skills")
async def most_skills(search_skill: str = "requiredSkills" , search_by: str = "name", limit: int = 10, offset: int = 0, order_in: str = "DESC"):
    # database = data_loader()
    # conn = database.create_connection('planning.db')
    try:
        print(search_skill)
        print(search_by)
        print(limit)
        print(offset)
        print(order_in)
        # search_skill = ""
        # skill_type = ""
        result = database.query(f'select skill, count(skill) as count from (select json_extract(json_each.value,"$.{search_by}") as skill from planning,json_each({search_skill})) group by skill order by count {order_in} limit {limit} offset {offset}',conn)
        # db = database.create_table('./Schema.sql', conn)
        # warehouse = database.format_and_load_json_data('./../planning.json',conn,'planning')
        print("********")
        print(result.shape)
        if result.shape[0] != 0:
            return {"success": True , "message": result.to_dict(orient='records')}
        else:
            return {"success": False , "message": "No Rows Found"}
    except Exception as e:
        return {"success": False , "message": "Please check your parameters" , "error_code": e }

@app.get("/talent_grade_count")
async def root():
    database = data_loader()
    conn = database.create_connection('planning.db')
    result = database.query("select talentGrade , count(distinct talentId) as count from planning group by talentGrade order by count ASC limit 5 OFFSET 2")
    # db = database.create_table('./Schema.sql', conn)
    # warehouse = database.format_and_load_json_data('./../planning.json',conn,'planning')
    return {"success": True , "message": result}

# @app.get("/most_listed_skills")
# async def root():
#     database = data_loader()
#     conn = database.create_connection('planning.db')
#     result = database.query("select talentGrade , count(distinct talentId) as count from planning group by talentGrade order by count ASC limit 5 OFFSET 2")
#     # db = database.create_table('./Schema.sql', conn)
#     # warehouse = database.format_and_load_json_data('./../planning.json',conn,'planning')
#     return {"success": True , "message": result}

@app.get("/most_listed_skills")
async def most_skills(search_column: str = "id" , search_by = 1, limit: int = 10, offset: int = 0 , order_in: str = "DESC", order_by: str = "id"):
    # database = data_loader()
    # conn = database.create_connection('planning.db')
    try:
        print(search_column)
        print(search_by)
        print(limit)
        print(offset)
        print(order_in)
        # search_skill = ""
        # skill_type = ""
        result = database.query(f'select * from planning where {search_column}={search_by} order by {order_by} {order_in} limit {limit} offset {offset}',conn)
        # db = database.create_table('./Schema.sql', conn)
        # warehouse = database.format_and_load_json_data('./../planning.json',conn,'planning')
        print("********")
        print(result.shape)
        if result.shape[0] != 0:
            return {"success": True , "message": result.to_dict(orient='records')}
        else:
            return {"success": False , "message": "No Rows Found"}
    except Exception as e:
        return {"success": False , "message": "Please check your parameters" , "error_code": e }

@app.get("/query")
async def query():
    return {"message": "Hello World"}
