

def extract_data(file_path, spark):
    '''
    Fungsi ini ditujukan untuk mengambil csv dari local directory untuk selanjutnya di load ke Pyspark

    Parameters:
        file_path   : membaca data (harap dicopy path nya)
        spark       : spark session

    Return:
        data        : pandas dataframe (csv) diload ke pyspark dataframe
    '''
    data = spark.read.csv(file_path, header=True, inferSchema=True)
    return data