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
  04MAR2026   RLEWIS  Added deduping and argparse option
----------------------------------------------------------------------------------------------------------------------:
"""

"""

Task: Clean the datasets and return two csv files

Write into a SQL Server

Create activity tracker

"""

# ---------------
# --- Imports ---
# ---------------

import pandas as pd
import numpy as np
import os, re, urllib, logging
from sqlalchemy import create_engine
import argparse
from pathlib import Path
from datetime               import datetime, date, timedelta  
from dateutil.relativedelta import relativedelta              

from common.functions import intck, enforce_int32, date_reason, duration_to_days, write_to_sql

    # ---------------

# ---------------
# --- Logging ---
# --------------- 

# this should ultimately go to a file
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

    # ---------------

# -----------------
# --- Functions ---
# ----------------- 

# initialise metric rows for Power BI use
metrics_rows = []
metric_order = 0

today = datetime.now().date()

def log_metric(metric_type: str, action: str, records: int, **extra):
    
    global metric_order

    metric_order += 1

    metrics_rows.append({"order_id": int(metric_order), "date": today, "type": metric_type, "action": action, "records": int(records), **extra})

    # ---------------

# ----------------
# --- argparse ---
# ----------------

# <<< NEED TO ADD OPTION FOR BOTH >>>

parser = argparse.ArgumentParser()

parser.add_argument(
    "--output_type",
    default = "csv",   # default when not provided
    choices = ["csv", "sql"]
)

args, unknown = parser.parse_known_args()

    # ---------------

# ------------
# --- Main ---
# ------------ 

if __name__ == '__main__': 

    # data directory
    data_dir = "C:\\Users\\Admin\\qa-library-fresh\\data"

    # scan data directory for files
    files = list(Path(data_dir).iterdir())

    df_files = pd.DataFrame({
        "filename": [f.name for f in files],
        "extension": [f.suffix for f in files]
    })

    # remove the output folder
    df_files = df_files[df_files['filename'] != 'output']

    files_detected = len(df_files.index)

    log_metric("files detected", "n/a", files_detected)

    # detect incorrect extensions
    incorrect_extensions = df_files[df_files['extension'] != '.csv'].copy()
    
    log_metric("files with incorrect extensions", "dropped", len(incorrect_extensions.index))

    if len(incorrect_extensions.index) > 0:

        logger.info("Files with incorrect extensions have been identified")

    # keep only csv
    df_files = df_files[df_files['extension'] == '.csv']

    if len(df_files.index) > 0:

        logger.info("Files have been detected")

        # here we know what files to expect, this is no always the case, would normally need additional checks 
        expected_files = ["03_Library Systembook.csv", "03_Library SystemCustomers.csv"]

        # are the detected files the expected files?
        valid_df_files = df_files[df_files["filename"].isin(expected_files)]

        log_metric("valid files detected", "n/a", len(valid_df_files.index))

        if len(valid_df_files.index) > 0:

            logger.info("Expected files detected")

            for fname in valid_df_files['filename']:

                # ------------------------
                # --- System Customers ---
                # ------------------------

                if fname == '03_Library SystemCustomers.csv':

                    logger.info("Processing System Customers")

                    # read in csv file                
                    df_system_customers = pd.read_csv(os.path.join(data_dir, fname))

                    log_metric(f"{fname}", "read in", len(df_system_customers.index))

                    # check columns
                    expected_customer_columns = ['Customer ID', 'Customer Name']
                    renamed_customer_columns  = ['customer_id', 'customer_name']

                    if list(df_system_customers.columns) == expected_customer_columns:

                        # what number do we want here?
                        log_metric(f"{fname}", "all columns correct", len(expected_customer_columns))

                        # rename columns
                        df_system_customers_rename = dict(zip(expected_customer_columns, renamed_customer_columns))   
                        df_system_customers = df_system_customers.rename(columns = df_system_customers_rename)

                        # Check for blank records in any column - should really have logic to check for partials here as well
                        mask_df_system_customers_blanks = (
                            (df_system_customers.isna().any(axis=1))
                        )

                        df_system_customers_blanks = df_system_customers[mask_df_system_customers_blanks].copy()
                        
                        log_metric(f"blank rows on {fname}", "dropped", len(df_system_customers_blanks.index))

                        if len(df_system_customers_blanks.index) > 0:

                            logger.info('Blank Records on System Customers file')

                        # remove blanks
                        df_system_customers = df_system_customers[~mask_df_system_customers_blanks]

                        if len(df_system_customers.index) > 0:
                            
                            # convert id column to int32
                            df_system_customers = enforce_int32(df_system_customers, 'customer_id')

                            # set name to string - should include checks here as well really
                            df_system_customers['customer_name'] = df_system_customers['customer_name'].astype('string')

                            # check for dupes

                            before_count = len(df_system_customers)

                            df_system_customers = df_system_customers.sort_values(by = ['customer_id', 'customer_name'])
                            df_system_customers = df_system_customers.drop_duplicates(subset = ['customer_id', 'customer_name'], keep = 'last')

                            after_count = len(df_system_customers)

                            duplicates_removed = before_count - after_count

                            log_metric(f"duplicate rows on {fname}", "dropped", duplicates_removed)

                            # output cleaned files
                            df_system_customers_records = len(df_system_customers.index)

                            log_metric(f"system_customers output to {args.output_type}", "output", df_system_customers_records)

                            # output csv
                            
                            if args.output_type == 'csv':
                                df_system_customers.to_csv(os.path.join(data_dir,'output\\system_customers.csv'), index=False)

                            # upload to SQL
                            
                            if args.output_type == 'sql':
                                sql_system_customers_rows_written = write_to_sql(df_system_customers, "system_customers", "dbo")
    
                                # check upload
                                if len(df_system_customers.index) == sql_system_customers_rows_written:
                                    logger.info("System Customers succesfully uploaded to SQL")

                                    log_metric(f"records written to SQL for {fname}", "success", sql_system_customers_rows_written)                                

                                else:
                                    logger.error(f"Record mismatch on System Customers SQL upload {sql_system_customers_rows_written} rows written instead of {len(df_system_customers.index)}")

                                    log_metric(f"records written to SQL for {fname}", "failure", sql_system_customers_rows_written)     

                        else:

                            logger.info('No non blank records on System Customers file')

                    else: 

                        logger.info("System Customers file does not have the correct columns in the correct order")

                # -------------------
                # --- System Book ---
                # -------------------

                if fname == '03_Library Systembook.csv':

                    logger.info("Processing System Books")

                    # read in csv file
                    df_system_book = pd.read_csv(os.path.join(data_dir, fname))

                    log_metric(f"{fname}", "read in", len(df_system_book.index))                    

                    # check columns
                    expected_book_columns = ['Id', 'Books', 'Book checkout',  'Book Returned', 'Days allowed to borrow', 'Customer ID' ]
                    renamed_book_columns  = ['loan_id', 'book_name', 'checkout_date', 'return_date', 'loan_period', 'customer_id']

                    if list(df_system_book.columns) == expected_book_columns:

                        # what number do we want here?
                        log_metric(f"{fname}", "all columns correct", len(expected_book_columns))

                        # rename columns
                        df_system_book_rename = dict(zip(expected_book_columns, renamed_book_columns))   
                        df_system_book = df_system_book.rename(columns = df_system_book_rename)

                        # Check for blank records in any column apart from return_date
                        mask_df_system_book_blanks = (
                            (df_system_book.drop(columns=["return_date"]).isna().any(axis=1))
                        )

                        df_system_book_blanks = df_system_book[mask_df_system_book_blanks].copy()
                        
                        log_metric(f"blank rows on {fname}", "dropped", len(df_system_book_blanks.index))

                        if len(df_system_book_blanks.index) > 0:

                            logger.info('Blank Records on System Book file')

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
                            df_system_book['checkout_date'] = df_system_book['checkout_date'].astype('date32[pyarrow]')

                            if pd.api.types.is_string_dtype(df_system_book['return_date']):
                                df_system_book['return_date'] = df_system_book['return_date'].str.replace(r'[^0-9:/\-\s]', '', regex=True)
                                df_system_book['return_date'] = df_system_book['return_date'].str.strip()

                            # create raw date version
                            df_system_book['return_date_raw']   = df_system_book['return_date']

                            df_system_book['return_date'] = pd.to_datetime(df_system_book['return_date'], errors='coerce', dayfirst=True)
                            df_system_book['return_date'] = df_system_book['return_date'].astype('date32[pyarrow]')

                            # review dates - NEED TO INCLUDE CHECKS ON FUTURE DATES and RETURN BEFORE CHECKOUT
                            mask_null_system_book_dates = (
                                (pd.isna(df_system_book['checkout_date'])) |
                                (pd.isna(df_system_book['return_date']))
                            )
                            null_system_book_dates = df_system_book[mask_null_system_book_dates].copy()
                        
                            log_metric(f"null date rows on {fname}", "dropped", len(null_system_book_dates.index))

                            if len(null_system_book_dates.index) > 0:

                                logger.info("Issues detected with Date fields")

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
                        
                            log_metric(f"invalid Loan Period rows on {fname}", "dropped", len(df_system_book_invalid_loan_days.index))

                            if len(df_system_book_invalid_loan_days.index) > 0:

                                logger.info("Issues detected with Loan Period field")

                            # remove any loan period issues
                            df_system_book = df_system_book[~mask_invalid_loan_period]

                            # actual loaned days intck
                            df_system_book['actual_loaned_days'] = intck('day', df_system_book['checkout_date'], df_system_book['return_date'])

                            # keep fields in order
                            df_system_book = df_system_book[['loan_id', 'customer_id', 'book_name', 'checkout_date', 'return_date', 'loan_days', 'actual_loaned_days']]

                            # check for dupes
                            before_count = len(df_system_book)

                            df_system_book = df_system_book.sort_values(by = ['loan_id', 'customer_id', 'book_name', 'checkout_date', 'return_date' ])
                            df_system_book = df_system_book.drop_duplicates(subset = ['loan_id', 'customer_id', 'book_name', 'checkout_date'], keep = 'last')

                            after_count = len(df_system_book)

                            duplicates_removed = before_count - after_count

                            log_metric(f"duplicate rows on {fname}", "dropped", duplicates_removed)

                            # output cleaned files

                            # output csv
                            
                            if args.output_type == 'csv':
                                df_system_book.to_csv(os.path.join(data_dir,'output\\system_book.csv'), index=False)

                            # upload to SQL
                            
                            if args.output_type == 'sql':
                                sql_system_book_rows_written = write_to_sql(df_system_book, "system_book", "dbo")
        
                                # check upload
                                if len(df_system_book.index) == sql_system_book_rows_written:

                                    logger.info("System Book succesfully uploaded to SQL")

                                    log_metric(f"records written to SQL for {fname}", "success", sql_system_book_rows_written)   

                                else:

                                    log_metric(f"records written to SQL for {fname}", "failure", sql_system_book_rows_written)   

                                    logger.error(f"Record mismatch on System Book SQL upload {sql_system_book_rows_written} rows written instead of {len(df_system_book.index)}")

                        else:

                            logger.info('No non blank records on System Book file')

                    else: 

                        logger.info("System Book file does not have the correct columns in the correct order")

            metrics_df = pd.DataFrame(metrics_rows)  

            if args.output_type == 'csv':

                metrics_df.to_csv(os.path.join(data_dir,'output\\metrics_tracking.csv'), index=False)

            if args.output_type == 'sql':

                metrics_df_rows_written = write_to_sql(metrics_df, "metrics_tracking", "dbo")

    else:

        logger.info("No Observations")

