import os
import psycopg2
from sqlalchemy import create_engine
from dotenv import load_dotenv

def load_data(datamart):
    '''
    Fungsi ini ditujukan untuk melakukan load data ke NeonDB    
    '''
    load_dotenv()

    engine = create_engine(os.getenv('DATABASE_URL'))
    load_data_to_db(datamart, "Datamart_1",engine)    



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