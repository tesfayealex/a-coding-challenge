from fastapi import FastAPI, Depends
from main import data_loader
from fastapi_pagination import Page, Params, paginate, add_pagination, LimitOffsetPage
import pandas as pd
from model_for_fastapi_pagination import Planning, PlanningOut
# import json
import uvicorn

app = FastAPI()
add_pagination(app)
database = data_loader()
conn, engine = database.create_connection('planning.db')

@app.get("/api/populate_data") # Api to load the data into sqlite
def root():
    db = database.create_table('./Schema.sql', engine)
    warehouse = database.format_and_load_json_data('./../planning.json',engine,'planning')
    return {"success": True , "message": "Data have been populated successfully"}

@app.get("/api/most_listed_skills") # get the most listed skill in required or optional skills columns plus accepts pagination methods as input
def most_skills(search_skill: str = "requiredSkills" , search_by: str = "name", limit: int = 10, offset: int = 0, order_in: str = "DESC"):
    try:
        result = database.query(f'select skill, count(skill) as count from (select json_extract(json_each.value,"$.{search_by}") as skill from planning,json_each({search_skill})) group by skill order by count {order_in} limit {limit} offset {offset}',engine)
        if result.shape[0] != 0:
            return {"success": True , "message": result.to_dict(orient='records')}
        else:
            return {"success": False , "message": "No Rows Found"}
    except Exception as e:
        return {"success": False , "message": "Please check your parameters" , "error_message": e }

@app.get("/api/talent_grade_count") # get the talent grade count in rtalent grade column plus accepts pagination methods as input
def talent_grade_count(limit: int = 10, offset: int = 0, order_in: str = "DESC"):
    try:
        result = database.query(f'select talentGrade , count(distinct talentId) as count from planning group by talentGrade order by count {order_in} limit {limit} OFFSET {offset}',engine)
        if result.shape[0] != 0:
            return {"success": True , "message": result.to_dict(orient='records')}
        else:
            return {"success": False , "message": "No Rows Found"}
    except Exception as e:
        return {"success": False , "message": "Please check your parameters" , "error_message": e }


@app.get(path="/api/plannings/all", name="Gets all plannings", response_model=Page[PlanningOut]) # get all plannings as a test for fast-api's pagination feature
async def get_all_plannings():
    conn = database.create_db_session('planning.db')
    results = conn.query(Planning).all()
    return paginate(results)

@app.get(path="/api/plannings/all_with_limit", name="Gets all plannings", response_model=LimitOffsetPage[PlanningOut]) # get all plannings as a test for fast-api's pagination feature with limit and offset input
async def get_all_employees():
    conn = database.create_db_session('planning.db')
    results = conn.query(Planning).all()
    return paginate(results)

@app.get("/api/filter") # get all plannings based on a filter and pagination methos
async def filter_with_column(search_column: str = "id" , search_by = 1, limit: int = 10, offset: int = 0 , order_in: str = "DESC", order_by: str = "id"):
    try:
        result = database.query(f'select * from planning where {search_column}={search_by} order by {order_by} {order_in} limit {limit} offset {offset}',engine)
        if result.shape[0] != 0:
            return {"success": True , "message": result.to_dict(orient='records')}
        else:
            return {"success": False , "message": "No Rows Found"}
    except Exception as e:
        return {"success": False , "message": "Please check your parameters" , "error_code": e }

@app.get("/") # welcome page with the first instruction
async def query():
    return {"message": "Welcome to The Coding Challange Solution you can start by populating the data with /api/populate_data"}

if __name__=='__main__':
    uvicorn.run("api:app", host="127.0.0.1", port=8000, reload=True) # starts the fastapi at port 8000
