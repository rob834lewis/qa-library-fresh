# -*- coding: utf-8 -*-
"""
----------------------------------------------------------------------------------------------------------------------
  Written by      : Rob Lewis

  Date            : 03MAR2026

  Purpose         : Data pipeline

  Dependencies    :

  Module name    : pipeline

  Modifications
  -------------
  03MAR2026   RLEWIS  Initial Version
----------------------------------------------------------------------------------------------------------------------:
"""

"""

Task: Clean the datasets and return two csv files

Want to see ETL pipe that takes the data and output clean ones

Write into a SQL Server

"""

# ---------------
# --- Imports ---
# ---------------

import pandas as pd
import numpy as np
import os, re
import urllib
from sqlalchemy import create_engine

    # ---------------

# -----------------
# --- Functions ---
# ----------------- 

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

def date_reason(s: str) -> str:
    if s is None:
        return "Missing"
    s = str(s).strip()
    if s == "":
        return "Missing"
    m = re.fullmatch(r"(\d{1,2})/(\d{1,2})/(\d{2,4})", s)
    if not m:
        return "Unexpected format"
    d, mo, y = map(int, m.groups())
    if mo < 1 or mo > 12:
        return "Month out of range"
    if d < 1 or d > 31:
        return "Day out of range"
    return "Invalid date (e.g., day/month mismatch)"

day_conversion_map = {
    "day": 1,
    "days": 1,
    "week": 7,
    "weeks": 7,
    "month": 30,
    "months": 30,
    "year": 365,
    "years": 365
}

def duration_to_days(value):
    if pd.isna(value):
        return pd.NA
    
    value = str(value).strip().lower()
    
    match = re.fullmatch(r"(\d+)\s*(day|days|week|weeks|month|months|year|years)", value)
    
    if not match:
        return pd.NA  # flag invalid
    
    number = int(match.group(1))
    unit = match.group(2)
    
    return number * day_conversion_map[unit]

    # ---------------

# ----------------------
# --- SQL Connection ---
# ----------------------


server   = "localhost"
database = "Library"
driver   = "ODBC Driver 17 for SQL Server"

params = urllib.parse.quote_plus(
    f"DRIVER={{{driver}}};"
    f"SERVER={server};"
    f"DATABASE={database};"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;"
)

engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}", fast_executemany=True)

    # ---------------


# ------------
# --- Main ---
# ------------ 

# <<< Need logging >>>

# data directory
data_dir = "C:\\Users\\Admin\\qa-library-fresh\\data"

# scan data directory for files
df_files = pd.DataFrame(os.listdir(data_dir), columns=["filename"])

if len(df_files.index) > 0:

    print("Files have been detected")

    # here we know what files to expect, this is no always the case, would normally need additional checks 
    expected_files = ["03_Library Systembook.csv", "03_Library SystemCustomers.csv"]

    # are the detected files the expected files?
    valid_df_files = df_files[df_files["filename"].isin(expected_files)]

    if len(valid_df_files.index) > 0:

        print("Expected files detected")

        for fname in valid_df_files['filename']:

            # ------------------------
            # --- System Customers ---
            # ------------------------

            if fname == '03_Library SystemCustomers.csv':

                print("Processing System Customers")

                # read in csv file                
                df_system_customers = pd.read_csv(os.path.join(data_dir, fname))

                # check columns
                expected_customer_columns = ['Customer ID', 'Customer Name']
                renamed_customer_columns  = ['customer_id', 'customer_name']

                if list(df_system_customers.columns) == expected_customer_columns:

                    # rename columns
                    df_system_customers_rename = dict(zip(expected_customer_columns, renamed_customer_columns))   
                    df_system_customers = df_system_customers.rename(columns = df_system_customers_rename)

                    # Check for blank records in any column - should really have logic to check for partials here as well
                    mask_df_system_customers_blanks = (
                        (df_system_customers.isna().any(axis=1))
                    )

                    df_system_customers_blanks = df_system_customers[mask_df_system_customers_blanks].copy()

                    if len(df_system_customers_blanks.index) > 0:

                        print('Blank Records on System Customers file')

                    # remove blanks
                    df_system_customers = df_system_customers[~mask_df_system_customers_blanks]

                    if len(df_system_customers.index) > 0:
                           
                        # convert id column to int32
                        df_system_customers = enforce_int32(df_system_customers, 'customer_id')

                        # set name to string - should include checks here as well really
                        df_system_customers['customer_name'] = df_system_customers['customer_name'].astype('string')

                        # UPLOAD TO SQL HERE
                        table_name = "system_customers"
                        schema     = "dbo"  

                        sql_system_customers_rows_written = df_system_customers.to_sql(
                            name=table_name,
                            con=engine,
                            schema=schema,
                            if_exists="replace",   # "fail" | "replace" | "append"
                            index=False,
                            chunksize=5000,       
                            method="multi"        
                        )

                    else:

                        print('No non blank records on System Customers file')

                else: 

                    print("System Customers file does not have the correct columns in the correct order")

            # -------------------
            # --- System Book ---
            # -------------------

            if fname == '03_Library Systembook.csv':

                print("Processing System Books")

                # read in csv file
                df_system_book = pd.read_csv(os.path.join(data_dir, fname))

                # check columns
                expected_book_columns = ['Id', 'Books', 'Book checkout',  'Book Returned', 'Days allowed to borrow', 'Customer ID' ]
                renamed_book_columns  = ['loan_id', 'book_name', 'checkout_date', 'return_date', 'loan_period', 'customer_id']

                if list(df_system_book.columns) == expected_book_columns:

                    # rename columns
                    df_system_book_rename = dict(zip(expected_book_columns, renamed_book_columns))   
                    df_system_book = df_system_book.rename(columns = df_system_book_rename)

                    # Check for blank records in any column apart from return_date
                    mask_df_system_book_blanks = (
                        (df_system_book.drop(columns=["return_date"]).isna().any(axis=1))
                    )

                    df_system_book_blanks = df_system_book[mask_df_system_book_blanks].copy()

                    if len(df_system_book_blanks.index) > 0:

                        print('Blank Records on System Book file')

                    # remove blanks
                    df_system_book = df_system_book[~mask_df_system_book_blanks]

                    if len(df_system_book.index) > 0:
                           
                        # convert data types

                        # convert id columns to int32
                        df_system_book = enforce_int32(df_system_book, 'loan_id')
                        df_system_book = enforce_int32(df_system_book, 'customer_id')

                        # set name to string - should include checks here as well really
                        df_system_book['book_name'] = df_system_book['book_name'].astype('string')

                        # resolve date fields
                        if pd.api.types.is_string_dtype(df_system_book['checkout_date']):
                            df_system_book['checkout_date'] = df_system_book['checkout_date'].str.replace(r'[^0-9:/\-\s]', '', regex=True)
                            df_system_book['checkout_date'] = df_system_book['checkout_date'].str.strip()

                        # create raw date version
                        df_system_book['checkout_date_raw'] = df_system_book['checkout_date']

                        # convert date
                        df_system_book['checkout_date'] = pd.to_datetime(df_system_book['checkout_date'], errors='coerce', dayfirst=True)

                        if pd.api.types.is_string_dtype(df_system_book['return_date']):
                            df_system_book['return_date'] = df_system_book['return_date'].str.replace(r'[^0-9:/\-\s]', '', regex=True)
                            df_system_book['return_date'] = df_system_book['return_date'].str.strip()

                        # create raw date version
                        df_system_book['return_date_raw']   = df_system_book['return_date']

                        df_system_book['return_date'] = pd.to_datetime(df_system_book['return_date'], errors='coerce', dayfirst=True)

                        # review dates
                        mask_null_system_book_dates = (
                            (pd.isna(df_system_book['checkout_date'])) |
                            (pd.isna(df_system_book['return_date']))
                        )
                        null_system_book_dates = df_system_book[mask_null_system_book_dates].copy()

                        if len(null_system_book_dates.index) > 0:

                            print("Issues detected with Date fields")

                            null_system_book_dates["checkout_date_reason"] = null_system_book_dates["checkout_date_raw"].apply(date_reason)
                            null_system_book_dates["return_date_reason"]   = null_system_book_dates["return_date_raw"].apply(date_reason)

                        # remove any date issue lines
                        df_system_book = df_system_book[~mask_null_system_book_dates]

                        # convert loan period
                        # df_system_book['loan_period'].value_counts(dropna=False)

                        df_system_book["loan_days"] = df_system_book["loan_period"].apply(duration_to_days).astype("int32")

                        mask_invalid_loan_period = (
                            (pd.isna(df_system_book["loan_days"]))
                        )
                        df_system_book_invalid_loan_days = df_system_book[mask_invalid_loan_period].copy()

                        if len(df_system_book_invalid_loan_days.index) > 0:

                            print("Issues detected with Loan Period field")

                        # remove any loan period issues
                        df_system_book = df_system_book[~mask_invalid_loan_period]

                        # keep fields in order
                        df_system_book = df_system_book[['loan_id', 'customer_id', 'book_name', 'checkout_date', 'return_date', 'loan_days']]
                        
                        # UPLOAD TO SQL HERE
                        table_name = "system_book"
                        schema     = "dbo"  

                        sql_system_book_rows_written = df_system_book.to_sql(
                            name=table_name,
                            con=engine,
                            schema=schema,
                            if_exists="replace",   # "fail" | "replace" | "append"
                            index=False,
                            chunksize=5000,       
                            method="multi"        
                        )

                        # IMPLEMENT CHECK ON RECORDS UPLOADED

                    else:

                        print('No non blank records on System Book file')

                else: 

                    print("System Book file does not have the correct columns in the correct order")


                # info
                df_system_book.info()



else:

    print("No Observations")

