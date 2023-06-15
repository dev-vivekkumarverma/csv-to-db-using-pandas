from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import pandas as pd
import time 


PASSWORD=#
DBURL='postgresql://postgres:{}@localhost:5432/{}'.format(quote_plus(PASSWORD),"CSVDATA")
DBEngine=create_engine(DBURL)
table_name='DATA_BULK'
with DBEngine.connect() as connection:
    try:
        start_time=time.time()
        create_table_sql="""CREATE TABLE IF NOT EXISTS {}(
            id integer not null primary key,
            email varchar(50),
            first_name varchar(50),
            last_name varchar(50)

        )""".format(table_name)
        connection.execute(text(create_table_sql))
        # connection.commit()
        print("-"*50)
        print("Database table created with the name of {}".format(table_name))
        print("_"*50)

        print("reading csv file...")
        dataframe=pd.read_csv("./convertcsv.csv")
        start=0
        chunk_size=50
        for i in range(chunk_size-1,(dataframe.shape)[0],chunk_size):
            chunck_data=dataframe.loc[start:i,:]
            print("data From {} to {} read".format(start+1,i+1))
            list_of_dictionary=[chunck_data.iloc[index,:].to_dict() for index in range(chunk_size)]
            print("convering the data into SQL statments")
            insert_sql="""INSERT INTO {} (id,email, first_name,last_name) VALUES(:id,:email,:first_name,:last_name)""".format(table_name)
            connection.execute(text(insert_sql),list_of_dictionary)
            # connection.commit()
            start=i+1
        time_taken=time.time() - start_time
        print("Time taken in complete execution of the process is {} s".format(time_taken))
    except Exception as e:
        print(str(e))