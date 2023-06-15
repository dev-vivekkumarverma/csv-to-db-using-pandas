# from dotenv import load_dotenv
from urllib.parse import quote_plus
from sqlalchemy import create_engine,text
# from sqlalchemy import insert,select
import pandas as pd
import time
# import os

# if os.path.exists(".env"):
#     load_dotenv(".env")
# else:
#     raise FileNotFoundError(".env file is Missing")
PASSWORD=#
engine = create_engine('postgresql://postgres:{}@localhost:5432/{}'.format(quote_plus(PASSWORD),"CSVDATA"))
# create_table_sql="""CREATE TABLE IF NOT EXISTS DATA(id SERIAL PRIMARY KEY,EMAIL varchar(50),firstname varchar(50),lastname varchar(50));"""
connection=engine.connect()

create_table_sql = """
    CREATE TABLE IF NOT EXISTS DATA (
        id INTEGER PRIMARY KEY NOT NULL,
        email VARCHAR(50),
        first_name VARCHAR(50),
        last_name VARCHAR(50)
    )
"""

# Establish the connection
with engine.connect() as connection:
    try:
        start_time=time.time()
        # Execute the create table statement
        connection.execute(text(create_table_sql))
        # connection.commit()
        print("~"*50)
        print("Table created successfully.")

        # read csv file
        DataDF= pd.read_csv("./convertcsv.csv")
        print("Data read successfully from csv")
        print("~"*50)
        print("\nNow it's time to insert data")
     

        for index in range((DataDF.shape)[0]):
            data=DataDF.iloc[index,:].to_dict()
            print("data:",data)
            delete_sql="DELETE from DATA"
            connection.execute(text(delete_sql))
            insert_sql="INSERT INTO DATA(id, email, first_name, last_name) VALUES ({},'{}','{}','{}')".format(data['id'],data['email'],data["first_name"],data["last_name"])
         
            connection.execute(text(insert_sql))

            print("Done !")
        # connection.commit()

        print("time taken:{} s".format(time.time()-start_time))
    except Exception as e:
        print("Error occurred:", str(e))