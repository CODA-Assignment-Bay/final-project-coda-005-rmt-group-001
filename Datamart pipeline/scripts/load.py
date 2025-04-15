from pyspark.sql import SparkSession
from pymongo import MongoClient

def load_data(df, collection_name):
    '''
    Fungsi ini ditujukan untuk melakukan load data ke MongoDB

    Parameter:
        df: dataframe -  tabel yang berisikan semua data
    
    '''
    mongodb_uri="mongodb+srv://admin:admin@doni-playground.cv49f.mongodb.net"
    database_name = "Soil-Polution"
    client = MongoClient(mongodb_uri)
    db = client[database_name]
    collection = db[collection_name]

    #mengkonversi table menjadi list of dictionary yang dapat diterima mongodb
    data = []
    rows = df.collect()
    for row in rows:
        row_dict = row.asDict()
        data.append(row_dict)

    print("data length", len(data))
    #data tidak kosong
    if data:
        collection.insert_many(data)
    else:
        print("data kosong")
