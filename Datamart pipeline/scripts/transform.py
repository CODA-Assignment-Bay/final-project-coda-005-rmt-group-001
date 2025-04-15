from pyspark.sql.functions import to_timestamp, col, when


def transform_data(df ,spark):
    '''
    Fungsi ini ditujukan untuk mengambil csv dari local directory untuk selanjutnya di load ke Pyspark

    Parameters:
        df        : data yang telah diekstrak
        spark       : spark session

    Return:
        data        : pandas dataframe (csv) diload ke pyspark dataframe

    '''
    df = drop_column(df, "Region")
    df = convert_yes_no_to_boolean(df, ["Case_Resolved","Follow_Up_Required"])
    df = drop_missing_string_date(df)
    df = filter_none(df)
    df = convert_datetime(df, ["Date_Reported"], "yyyy-MM-dd")
    
    
    #Membuat tabel Pollutant
    pollutant = df.select("Case_ID",
                      "Pollutant_Type",
                      "Pollutant_Concentration_mg_kg")
    pollutant = create_custom_id(pollutant, "Pollutant","Pol-","Case_ID", spark)
    
    #membuat tabel Soil
    soil = df.select("Case_ID",
                 "Soil_pH",
                 "Temperature_C",
                 "Humidity_%",
                 "Rainfall_mm",
                 "Soil_Texture", 
                 "Soil_Organic_Matter_%")
    
    soil  = create_custom_id(soil, "Soil","Soil-","Case_ID", spark)


    #membuat tabel Farm
    farm = df.select("Case_ID",
                 "Crop_Type",
                 "Farming_Practice",
                 "Water_Source_Type")
    
    farm  = create_custom_id(farm, "Farm","Farm-","Case_ID", spark)
    
    #membuat tabel disease
    disease = df.select("Case_ID",
                    "Nearby_Industry",
                    "Disease_Type",
                    "Disease_Severity",
                    "Health_Symptoms",
                    "Age_Group_Affected", 
                    "Gender_Most_Affected")
    disease  = create_custom_id(disease, "Disease","Disease-","Case_ID",spark)
    

    #membuat tabel case
    case = df.select("Case_ID",
                 "Date_Reported",
                 "Country",
                 "Mitigation_Measure",
                 "Case_Resolved", 
                 "Follow_Up_Required")
    
    #melakukan join ke tabel case sehingga menjadi fact table
    case = case.alias("c").join(
    pollutant.alias("p"), col("c.Case_ID") == col("p.Case_ID"), "inner"
    ).join(
        soil.alias("s"), col("c.Case_ID") == col("s.Case_ID"), "inner"
    ).join(
        farm.alias("f"), col("c.Case_ID") == col("f.Case_ID"), "inner"
    ).join(
        disease.alias("d"), col("c.Case_ID") == col("d.Case_ID"), "inner"
    ).select(
        col("c.Case_ID"), col("c.Date_Reported"), col("c.Country"), 
        col("p.Pollutant_ID"),
        col("s.Soil_ID"),
        col("f.Farm_ID"),
        col("d.Disease_ID"),
        col("c.Mitigation_Measure"), col("c.Case_Resolved"), col("c.Follow_Up_Required"), 
    )
    
    #membuat koleksi data yang akan menjadi datawarehouse
    datawarehouse_set = {}
    datawarehouse_set["Pollutant"] = pollutant
    datawarehouse_set["Soil"] = soil
    datawarehouse_set["Farm"] = farm
    datawarehouse_set["Disease"] = disease
    datawarehouse_set["Case"] = case

    return datawarehouse_set

def create_custom_id(dataframe, dataframe_name, id_code, reference_coulumn_name, spark):
    dataframe.createOrReplaceTempView(dataframe_name+"_view")
    dataframe = spark.sql(
        f"select concat('{id_code}',row_number() over (order by '{reference_coulumn_name}')) as {dataframe_name}_ID,*  from {dataframe_name}_view"
        )
    return dataframe

def drop_column(df, column_name):
    '''
    Fungsi ini ditujukan untuk membuang kolom yang tidak akan digunakan di data warehouse

    Parameters:
        df           : nama dataframe yang telah diload ke pyspark
        column_name  : nama kolom yang akan di-drop

    Return:
        df           : dataframe dengan tanpa kolom yang telah di drop

    Contoh penggunaan:
        df = drop_column(df,"Ticket_id")
        kolom "Ticket_id" akan di-drop dari dataset bernama df
    '''
    df = df.drop(column_name)
    return df

def filter_none(df):
    """
    Fungsi ini ditujukan untuk drop kolom dengan values 'None'

    Parameters:
        df   : nama dataframe yang telah diload ke pyspark

    Return:
        df  : dataframe tanpa baris yang value nya 'None'

    Contoh penggunaan:
        df = filter_none(df)
        dataframe df tidak mempunyai kolom yang barisnya 'None'
        
    """
    # Get string-type columns
    string_cols = [c for c, c_type in df.dtypes if c_type == 'string']
    
    # Apply filter for each string column
    for column in string_cols:
        df = df.filter(col(column) != 'None')
    
    return df

def drop_missing_string_date(df):
    """
    Fungsi ini ditujukan untuk drop kolom dengan null untuk string NaN untuk date 

    Parameters:
        df                      : nama dataframe yang telah diload ke pyspark

    Return:
        df_dropped_string_date  : dataframe tanpa baris yang value nya null & NaN (untuk tipe data string dan date) 

    Contoh penggunaan:
        df = drop_missing_string_date(df)
        dataframe df tidak mempunyai kolom yang barisnya null & NaN
    
    """
    # Mengambil kolom yang bertipe data string dan date
    target_cols = [c for c, c_type in df.dtypes if c_type in ('string', 'date')]

    # Membuang row yang memiliki tipe data string dan date yang null
    df_dropped_string_date = df.dropna(subset=target_cols)

    return df_dropped_string_date

def convert_yes_no_to_boolean(df, columns):
    """
    Fungsi ini ditujukan untuk mengubah tipe data string (Yes/No) ke tipe boolean

    Parameters:
        df           : nama dataframe yang telah diload ke pyspark
        columns      : nama kolom yang akan diubah tipe datanya

    Return:
        df           : dataframe dengan kolom bervalue "Yes" & "No" menjadi True & False

    Contoh penggunaan:
        df = convert_yes_no_to_boolean(df, ["Is_married"])
        kolom "Is_married" akan diubah tipe datanya dari Yes ke True dan No ke False
    """
    for column in columns:
        df = df.withColumn(
            column,
            when(col(column) == "Yes", True)
            .when(col(column) == "No", False)
            .otherwise(None)
        )
    return df

def convert_datetime(df, columns, format="yyyy-MM-dd"):
    '''
    Fungsi ini ditujukan untuk mengubah tipe data ke tipe datetime

    Parameters:
        df           : DataFrame yang telah diload ke PySpark
        columns      : List nama kolom yang akan diubah tipe datanya
        format       : Format datetime (default "yyyy-MM-dd")

    Return:
        df           : DataFrame dengan kolom yang telah dikonversi ke datetime

    Contoh penggunaan:
        df = convert_datetime(df, ["Purchased Date", "Sold Date"], "yyyy-MM-dd")
    '''
    for column in columns:
        df = df.withColumn(column, to_timestamp(col(column), format))
    return df