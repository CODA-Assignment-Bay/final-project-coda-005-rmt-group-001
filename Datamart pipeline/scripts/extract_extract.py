import pandas as pd

def extract_data(filename):
    """
    Fungsi ini ditujukan untuk memuat file CSV ke pandas dataframe.

    Parameters:
        filename (str): Nama file CSV.

    Returns:
        pd.DataFrame: DataFrame yang sudah di load.

    Contoh penggunaan:
        df = load_data("c/user/titanic.csv")
    """
    df = pd.read_csv(filename)
    df.columns = ['_'.join(word.capitalize() for word in name.split()) for name in df.columns]    
    df.columns = [clean_column(c) for c in df.columns]
    return df

def clean_column(col_name):
    return col_name.split('(')[0].strip()