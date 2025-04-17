import pandas as pd

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

#df_measurement_sample_piv = pivot_and_rename(df_measurement_sample)

def merge_pivot(df_pivoted, df_station):
    '''
    Fungsi ini menggabungkan table pivot dan table station

    Parameters:
        df_pivoted : DataFrame (measurement) yang sudah di pivot
        df_station : DataFrame yang berisi informasi stasiun

    Returns:
        summary    : DataFrame hasil merge keduanya

    contoh penggunaan:
        measurement_pivoted = pivot_and_rename(measurement_2)
    '''
    # Merge dengan station 
    summary = pd.merge(df_pivoted, df_station, on='Station_Code', how='left')

    # Memperbaiki ukuran tabel
    summary = summary[['Measurement_Date','Station_Code','Address','Latitude','Longitude','SO2','NO2','O3','CO','PM10','PM2.5']]
    
    return summary

#df_merged = merge_pivot(df_measurement_sample_piv, df_station)

def process_summary(df):
    df['Measurement_Date'] = pd.to_datetime(df['Measurement_Date'])
    df['Measurement_Time'] = df['Measurement_Date'].dt.time

    def extract_district(address):
        try:
            parts = address.split(', ')
            return parts[2]
        except:
            return None

    df['District'] = df['Address'].apply(extract_district)

    # Reorder kolom
    new_cols = ['Measurement_Date', 'Measurement_Time', 'Station_Code', 'Address', 'District',
                'Latitude', 'Longitude', 'SO2', 'NO2', 'O3', 'CO', 'PM10', 'PM2.5']
    
    return df[new_cols]

#df_summary = process_summary(df_merged)

def AQI_parameter(df):
    def calculate_aqi_pm25(pm25):
        if pm25 <= 30:
            return round((50 / 30) * pm25, 1)
        elif pm25 <= 60:
            return round(((100 - 51) / (60 - 31)) * (pm25 - 31) + 51, 1)
        elif pm25 <= 90:
            return round(((200 - 101) / (90 - 61)) * (pm25 - 61) + 101, 1)
        elif pm25 <= 120:
            return round(((300 - 201) / (120 - 91)) * (pm25 - 91) + 201, 1)
        elif pm25 <= 250:
            return round(((400 - 301) / (250 - 121)) * (pm25 - 121) + 301, 1)
        else:
            return round(((500 - 401) / (350 - 251)) * (pm25 - 251) + 401, 1)

    df['AQI_index'] = df['PM2.5'].apply(calculate_aqi_pm25)

    def categorize_aqi(aqi):
        if aqi <= 50:
            return "Good"
        elif aqi <= 100:
            return "Satisfactory"
        elif aqi <= 200:
            return "Moderate"
        elif aqi <= 300:
            return "Poor"
        elif aqi <= 400:
            return "Very Poor"
        else:
            return "Severe"

    df['AQI_category'] = df['AQI_index'].apply(categorize_aqi)
    return df 
