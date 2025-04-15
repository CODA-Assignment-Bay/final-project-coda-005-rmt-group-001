import os
import psycopg2
from sqlalchemy import create_engine
from dotenv import load_dotenv

def load_data(station_df, item_df, info_df, date_df):
    '''
    Fungsi ini ditujukan untuk melakukan load data ke MongoDB

    Parameter:
        df: dataframe -  tabel yang berisikan semua data
    
    '''

    load_dotenv()


    engine = create_engine(os.getenv('DATABASE_URL'))
    load_data_to_db(station_df, "Dim_Station",engine)
    load_data_to_db(item_df, "Dim_Item",engine)
    #load_data_to_db(info_df, "Fact_Info",engine)
    load_data_to_db(date_df, "Dim_Date",engine)


   


    


def get_data_from_db(query, engine):
    try:
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        print(e)

def load_data_to_db(df, table_name, engine):
    try:
        df.to_sql(table_name, engine, if_exists="append", index=False)
    except Exception as e:
        print(e)