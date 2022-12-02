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
            return conn,sqlite_engine
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
                result = conn.execute(query)
            return True
        except Exception as e: 
            return False
    def format_and_load_json_data(self,location,conn,sql_table):
        try:
            with open(location) as f:
                data = json.load(f)
            new_df = pd.DataFrame(data)
            new_df.to_sql(sql_table, con=conn,dtype={'requiredSkills': types.JSON, 'optionalSkills': types.JSON} ,if_exists='append',index=False)
            return True
        except Exception as e: 
            print(e)
            return False
    def query(self,query,conn):
        try:
            result = pd.read_sql(query,conn)
            print(result.head())
            return result
        except Exception as e: 
            print(e)
            return False


# if __name__ == "__main__":
#         database = data_loader()
#         conn = database.create_connection('planning.db')
#         db = database.create_table('./Schema.sql', conn)
#         warehouse = database.format_and_load_json_data('./../planning.json',conn,'planning')
#         query = 'select * from planning'
#         database.query(conn,query)

