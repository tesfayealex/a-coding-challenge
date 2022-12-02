from sqlalchemy import create_engine, types, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
import pandas as pd
import json


class data_loader:
    def create_connection(self,sql_db):
        try:
            sqlite_engine = create_engine(f'sqlite:///{sql_db}')
            conn = sqlite_engine.connect()
            # conn.exec_driver_sql(sql_query)
            return conn
        except Exception as e: 
            print(e)
            return False
    def create_db_session(self,sql_db):
        try:
            sqlite_engine = create_engine(f'sqlite:///{sql_db}')
            conn = sqlite_engine.connect()
            session = sessionmaker(bind=conn)
            conn = session()
            if session:
                return conn
            else:
                return False

        except SQLAlchemyError as se:
            print(se)
    def create_table(self,sql_query,conn):
        try:
            with open(sql_query) as file:
                query = text(file.read())
                conn.execute(query)
            return True
        except Exception as e: 
            print(e)
            return False
    def format_and_load_json_data(self,location,conn,sql_table):
        try:
            with open(location) as f:
                data = json.load(f)
            new_df = pd.DataFrame(data)
            # sqlite_engine = create_engine(f'sqlite:///{sql_db}')
            # conn = sqlite_engine.connect()
            new_df.to_sql(sql_table, con=conn,dtype={'requiredSkills': types.JSON, 'optionalSkills': types.JSON} ,if_exists='append',index=False)
            return True
        except Exception as e: 
            print(e)
            return False
    def query(self,query,conn):
        try:
            # sqlite_engine = create_engine(f'sqlite:///{sql_db}')
             # conn = sqlite_engine.connect()
        # result = conn.execute(query)
            result = pd.read_sql(query,conn)
            print(result.head())
            return result
        except Exception as e: 
            print(e)
            return False


if __name__ == "__main__":
        database = data_loader()
        conn = database.create_connection('planning.db')
        db = database.create_table('./Schema.sql', conn)
        warehouse = database.format_and_load_json_data('./../planning.json',conn,'planning')
        query = 'select * from planning'
        # query = "select distinct json_extract(json_each.value,'$.name') from planning,json_each(requiredSkills)  WHERE json_extract(json_each.value,'$.name')='German'"
        # query = 'select talentGrade , count(distinct talentId) from planning group by talentGrade'
        # query = 'select talentGrade , count(distinct talentId) as count from planning group by talentGrade order by count ASC limit 5'
        # query = 'select talentGrade , count(distinct talentId) as count from planning group by talentGrade order by count ASC limit 5'
        # query = 'select talentGrade , count(distinct talentId) as count from planning group by talentGrade order by count ASC limit 5 OFFSET 2'
        # query = 'select count(skill), skill from (select json_extract(json_each.value,'$.name') as skill from planning,json_each(requiredSkills)) group by skill'
        database.query(conn,query)

