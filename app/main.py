from sqlalchemy import create_engine, types, text,types
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
            new_df.to_sql(sql_table, con=conn,dtype={'requiredSkills': types.JSON, 'optionalSkills': types.JSON} ,if_exists='replace',index=False)
            return True
        except Exception as e: 
            print(e)
            return False
    def quering(self,conn,query):
        try:
            # sqlite_engine = create_engine(f'sqlite:///{sql_db}')
             # conn = sqlite_engine.connect()
        # result = conn.execute(query)
            result = pd.read_sql(query,conn)
            print(result.head())
        except Exception as e: 
            print(e)
            return False


if __name__ == "__main__":
        database = data_loader()
        conn = database.create_connection('planning.db')
        db = database.create_table('./Schema.sql', conn)
        warehouse = database.format_and_load_json_data('./../planning.json',conn,'planning')
        query = 'select * from planning'
        query = "select distinct json_extract(json_each.value,'$.name') from planning,json_each(requiredSkills)  WHERE json_extract(json_each.value,'$.name')='German'"
        # query = ''
        database.quering(conn,query)

