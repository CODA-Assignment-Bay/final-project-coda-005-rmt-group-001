import pandas as pd


def transform_data(measurement_info_df):
    '''
    Fungsi ini ditujukan untuk melakukan transformasi pada 3 tabel

    Parameters:
        measurement_info        : data yang telah diekstrak dari tabel measurement_info


    Return:
        measurement_info_df        : tabel measurement_info yang telah ditransformasi 
        measurement_recap_df       : tabel measurement_recap yang telah ditransformasi 
        dim_date                   : tabel dim_date yang telah ditransformasi 

    '''
    
    #transformasi pada tabel measurement_info
    measurement_info_df['Measurement_Date'] = pd.to_datetime(measurement_info_df['Measurement_Date']) #, format='%d%b%Y:%H:%M:%S.%f')
    measurement_info_df = measurement_info_df.drop("Instrument_Status", axis='columns')

    
    #creating dim date table
    date_range = pd.date_range(start='1/1/2017', end='1/1/2099', freq='H')
    dim_date = pd.DataFrame({'Timetable':date_range})
    dim_date['Year'] = dim_date.Timetable.dt.year
    dim_date['Month'] = dim_date.Timetable.dt.month
    dim_date['Day'] = dim_date.Timetable.dt.day
    dim_date['DayOfWeek'] = dim_date.Timetable.dt.dayofweek
    dim_date['WeekOfYear'] = dim_date.Timetable.dt.isocalendar().week
    dim_date['Hour'] = dim_date.Timetable.dt.hour
    dim_date['Quarter'] = dim_date.Timetable.dt.quarter
    dim_date['Half'] = dim_date.Timetable.dt.month.apply(lambda m: 1 if m<=6 else 2)

    return measurement_info_df, dim_date#, measurement_recap_df


def transform_measurement_info(df):
    '''
    Fungsi ini ditujukan untuk melaku
    '''
    pass

def transform_measurement_item(df):
    pass

def transform_measurement_station(df):
    pass



# Mengubah tipe data ke datetime
def convert_datetime(df, columns, format="%Y-%m-%d %H:%M"):
    '''
    Fungsi ini ditujukan untuk mengubah tipe data ke tipe datetime

    Parameters:
        df           : Nama dataframe
        columns      : List nama kolom yang akan diubah tipe datanya
        format       : Format datetime (default "%Y-%m-%d %H:%M")

    Return:
        df           : DataFrame dengan kolom yang telah dikonversi ke datetime

    Contoh penggunaan:
        df = convert_datetime(df, ["Measurement date"], "%Y-%m-%d %H:%M")
    '''
    for column in columns:
        df[column] = pd.to_datetime(df[column], format=format)
    return df

# measurement = convert_datetime(measurement, ['Measurement date'], "%Y-%m-%d %H:%M")
# -----------------------------------------------------------------------------------------------

# Drop kolom
def drop_column(df, column_name):
    '''
    Fungsi ini ditujukan untuk membuang kolom yang tidak akan digunakan di data warehouse

    Parameters:
        df           : Nama DataFrame
        column_name  : Nama kolom / list kolom yang akan di-drop

    Return:
        df           : DataFrame tanpa kolom yang telah di-drop

    Contoh penggunaan:
        df = drop_column(df, "Ticket_id")
        atau
        df = drop_column(df, ["Ticket_id", "User_ID"])
    '''
    df = df.drop(column_name, axis=1)
    return df

# measurement_2 = drop_column(measurement, "Instrument status")
# -----------------------------------------------------------------------------------------------

# Pivot_table
def pivot_and_rename(df):
    '''
    Fungsi ini mempivot tabel berdasarkan 'Measurement date' dan 'Station code' dengan 'Item code' sebagai kolom baru.

    Parameters:
        df : DataFrame yang akan di pivot

    Returns:
        df_pivoted : DataFrame yang telah dipivot dan diubah nama kolomnya

    contoh penggunaan:
        measurement_pivoted = pivot_and_rename(measurement_2)
    '''
    # Pivot table
    df_pivoted = pd.pivot_table(df,
                                index=['Measurement_Date', 'Station_Code'],
                                columns='Item_Code',
                                values='Average_Value')

    # Reset index agar kolom sejajar
    df_pivoted.columns = [f'{a}' for a in df_pivoted.columns]
    df_pivoted = df_pivoted.reset_index()

    # Rename Item Code berdasarkan Measurement Item
    rename_dict = {
        '1': 'SO2',
        '3': 'NO2',
        '5': 'CO',
        '6': 'O3',
        '8': 'PM10',
        '9': 'PM2.5'
    }
    df_pivoted = df_pivoted.rename(columns=rename_dict)

    return df_pivoted

#measurement_pivoted = pivot_and_rename(measurement_2)
# -----------------------------------------------------------------------------------------------

# Create date table
def generate_dim_date(start, end, freq):
    '''
    Fungsi ini membuat waktu berdasarkan rentang tertentu.

    Parameters:
        start : Tanggal awal ('YYYY-MM-DD').
        end   : Tanggal akhir ('YYYY-MM-DD').
        freq  : Frekuensi data ('H' = hourly).

    Returns:
        dim_date (DataFrame): DataFrame dengan kolom waktu dan atribut waktu terkait.

    contoh penggunaan:
        dim_date = generate_dim_date(start='2017-01-01', end='2022-01-01', freq='H')
    '''
    date_range = pd.date_range(start=start, end=end, freq=freq)
    dim_date = pd.DataFrame({'timetable': date_range})

    # Menarik informasi dari timteble dengan satuan waktu yang lain

    dim_date['year'] = dim_date['timetable'].dt.year
    dim_date['month'] = dim_date['timetable'].dt.month
    dim_date['day'] = dim_date['timetable'].dt.day
    dim_date['dayofweek'] = dim_date['timetable'].dt.dayofweek
    dim_date['weekofyear'] = dim_date['timetable'].dt.isocalendar().week
    dim_date['hour'] = dim_date['timetable'].dt.hour if freq.upper() == 'H' else None

    return dim_date