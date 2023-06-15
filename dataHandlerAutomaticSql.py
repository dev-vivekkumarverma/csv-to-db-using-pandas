from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import pandas as pd
import time

PASSWORD=#
DBURL='postgresql://postgres:{}@localhost:5432/{}'.format(quote_plus(PASSWORD),"CSVDATA")
DBEngine=create_engine(DBURL)
table_name="DATA_BULK_AUTOMATED_SQL_PANDAS"
with DBEngine.connect() as connection:
    try:
        start_time=time.time()
        print("reading csv file...")
        dataframe=pd.read_csv("./convertcsv.csv")
        print(dataframe.to_sql(name=table_name,con=connection, if_exists='replace', index=False))
        
        time_taken=time.time() - start_time
        print("time taken to complete entire process is {} s".format(time_taken))

        

    except Exception as e:
        print("error:",str(e))