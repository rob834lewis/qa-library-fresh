
import pandas as pd
import numpy  as np

def enforce_int32(df, col):
    
    """
    
    function to convert column to int32 

    Parameters:
    df  : the dataframe
    col : the column

    Returns:
    df with int32 formatted version of column

    """

    series = df[col]
    
    # Step 1 — If not numeric, attempt conversion
    if not pd.api.types.is_numeric_dtype(series):
        series = pd.to_numeric(series, errors="coerce")
    
    # Step 2 — Check for non-convertible values
    if series.isna().any():
        raise ValueError(f"{col} contains non-numeric values.")
    
    # Step 3 — Ensure values are whole numbers
    if not np.all(np.floor(series) == series):
        raise ValueError(f"{col} contains non-integer numeric values.")
    
    # Step 4 — Convert to int32
    df[col] = series.astype("int32")
    
    return df