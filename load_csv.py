
"""

Task: Clean the datasets and return two csv files

Want to see ETL pipe that takes the data and output clean ones

Write into a SQL Server

"""

import pandas as pd
import os

# Need logging

# data directory
data_dir = "C:\\Users\\Admin\\qa-library-fresh\\data"

# csv files
csv_system_book      = "03_Library Systembook.csv"
csv_system_customers = "03_Library SystemCustomers.csv"

# read in csv files
df_system_book      = pd.read_csv(os.path.join(data_dir, csv_system_book))
df_system_customers = pd.read_csv(os.path.join(data_dir, csv_system_customers))

# info
df_system_book.info()
df_system_customers.info()

# rename columns

df_system_book      = df_system_book.rename(columns = {'Id' : 'book_id', 'Books' : 'book_name', 'Book checkout' : 'checkout_date', 'Book Returned' : 'return_date', 'Days allowed to borrow' : 'loan_period', 'Customer ID' : 'customer_id'})

# ------------------------
# --- System Customers ---
# ------------------------

# check columns
expected_customer_columns = ['Customer ID', 'Customer Name']
renamed_customer_columns  = ['customer_id', 'customer_name']

if list(df_system_customers.columns) == expected_customer_columns:

    # rename columns
    df_system_customers_rename = dict(zip(expected_customer_columns, renamed_customer_columns))   
    df_system_customers = df_system_customers.rename(columns = df_system_customers_rename)

    # Check for blank records
    mask_df_system_customers_blanks = (
        (df_system_customers.isna().any(axis=1))
    )

    df_system_customers_blanks = df_system_customers[mask_df_system_customers_blanks].copy()

    if len(df_system_customers_blanks.index) > 0:
        print('Blank Records on System Customers file')

    # remove blanks
    df_system_customers = df_system_customers[~mask_df_system_customers_blanks]

    if len(df_system_customers.index) > 0:

        df_system_customers = df_system_customers

    else:

        print('No non blank records on System Customers file')

else: 

    print("System Customers file does not have the correct columns in the correct order")

# -------------------
# --- System Book ---
# -------------------

# convert data types

# id to int32 ?


if pd.api.types.is_string_dtype(df_system_book['checkout_date']):
    df_system_book['checkout_date'] = df_system_book['checkout_date'].str.replace(r'[^0-9:/\-\s]', '', regex=True)
    df_system_book['checkout_date'] = df_system_book['checkout_date'].str.strip()

df_system_book['checkout_date'] = pd.to_datetime(df_system_book['checkout_date'], errors='coerce', dayfirst=True)

if pd.api.types.is_string_dtype(df_system_book['return_date']):
    df_system_book['return_date'] = df_system_book['return_date'].str.replace(r'[^0-9:/\-\s]', '', regex=True)
    df_system_book['return_date'] = df_system_book['return_date'].str.strip()

df_system_book['return_date'] = pd.to_datetime(df_system_book['return_date'], errors='coerce', dayfirst=True)

