import psycopg2
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine(f"postgresql://neondb_owner:npg_f1uYCZ8nRWzQ@ep-floral-lake-a533zre9-pooler.us-east-2.aws.neon.tech/Air-Pollution-Seoul?sslmode=require")

def get_data_from_db(query):
    '''
    Fungsi ini ditujukan untuk mengambil file dari SQL cloud untuk selanjutnya di load ke pandas

    Parameters:
        query       : SQL query (engine: import dari sqlalchemy)
    
    Return:
        df          : csv dari SQL yang telah diload ke pandas dataframe

    Contoh penggunaan:
        query = "SELECT * FROM customers"
        df = get_data_from_db(query)
    '''
    try:
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        print(e)