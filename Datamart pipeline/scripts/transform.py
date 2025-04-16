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

def AQI_who(df):
    def classify_who_air_quality(row):
        violations = 0
        if row['SO2'] > 40:
            violations += 1
        if row['NO2'] > 25:
            violations += 1
        if row['CO'] > 4:  # CO dalam mg/m³
            violations += 1
        if row['O3'] > 100:
            violations += 1
        if row['PM10'] > 45:
            violations += 1
        if row['PM2.5'] > 15:
            violations += 1

        if violations == 0:
            return 'Baik'
        elif violations <= 2:
            return 'Sedang'
        elif violations <= 4:
            return 'Tidak Sehat'
        else:
            return 'Berbahaya'


    df['WHO_Air_Quality'] = df.apply(classify_who_air_quality, axis=1)
    return df

#datamart = AQI_who(df_summary)
