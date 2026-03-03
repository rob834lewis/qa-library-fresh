# -*- coding: utf-8 -*-
"""
----------------------------------------------------------------------------------------------------------------------
  Written by      : Rob Lewis

  Date            : 11SEP2025

  Purpose         : Create version of SAS intck function

  Dependencies    :

  Module name    : intck

  Modifications
  -------------
  11SEP2025   RLEWIS  Initial Version
----------------------------------------------------------------------------------------------------------------------:
"""

# ---------------
# --- Imports ---
# ---------------

import pandas as pd                 # For data manipulation
import numpy  as np                 # For scientific calculations
from datetime               import datetime, date, timedelta  # For generating dates
from dateutil.relativedelta import relativedelta              # For working with timedeltas

# -----------------
# --- Functions ---
# -----------------

def intck(interval, start_date, end_date):

    """
    Calculates the number of intervals between two dates.

    Parameters:
    interval (str): The interval type. Supported values: 'day', 'week', 'month', 'quarter', 'year'.
    start_date (str or datetime): The start date in the format 'YYYY-MM-DD' or a datetime object.
    end_date (str or datetime): The end date in the format 'YYYY-MM-DD' or a datetime object.

    Returns:
    int: The number of intervals between the start and end dates.
    """

    if any(type(var) == pd.Series for var in [start_date, end_date]):
        return intck_vectorised(interval, start_date, end_date)
       
    else:
        return intck_per_row(interval, start_date, end_date)

def intck_vectorised(interval, start_date, end_date):
        return_value = None
        if (type(start_date) != pd.Series and pd.isnull(start_date)) or (type(end_date) != pd.Series and pd.isnull(end_date)):
            return_value = np.nan
            return return_value

        interval = interval.lower()

        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, '%Y-%m-%d')

        elif isinstance(start_date, date):
            start_date = datetime.combine(start_date, datetime.min.time())

        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, '%Y-%m-%d')
       
        elif isinstance(end_date, date):
            end_date = datetime.combine(end_date, datetime.min.time())

        if type(start_date) == datetime:
            if type(end_date) == pd.Series:
                temp = [start_date for i in range(len(end_date.index))]
                start_date = pd.Series(temp)
            else:
                start_date = pd.Series(start_date)

        if type(end_date) == datetime:
            if type(start_date) == pd.Series:
                temp = [end_date for i in range(len(start_date.index))]
                end_date = pd.Series(temp)
            else:
                end_date = pd.Series(end_date)

        if start_date.dtype == date:
            start_date = pd.to_datetime(start_date)

        if end_date.dtype == date:
            end_date = pd.to_datetime(end_date)

        if interval == 'day':
            return_value = (end_date - start_date).dt.days

        if interval == 'week':
            #return_value = ((end_date - start_date).dt.days / 7).round().astype(int)
            monday1 = start_date - pd.to_timedelta(start_date.dt.weekday, unit='D')
            monday2 = end_date - pd.to_timedelta(end_date.dt.weekday, unit='D')
            return_value = ((monday2 - monday1).dt.days / 7).round().astype(int)

        if interval == 'month':
            start_year, start_month = start_date.dt.year, start_date.dt.month
            end_year, end_month = end_date.dt.year, end_date.dt.month
            return_value = (end_year - start_year) * 12 + (end_month - start_month)

        if interval == 'quarter':
            start_quarter = (start_date.dt.month - 1) // 3 + 1
            end_quarter = (end_date.dt.month - 1) // 3 + 1
            start_year, end_year = start_date.dt.year, end_date.dt.year
            return_value = (end_year - start_year) * 4 + (end_quarter - start_quarter)

        if interval == 'year':
            return_value = end_date.dt.year - start_date.dt.year

        if type(return_value) == pd.Series and return_value.size == 1:
            return return_value[0]
        elif type(return_value) != None:
            return return_value
        elif return_value == None:
            raise ValueError('Invalid interval type. Supported values: day, week, month, quarter, year.')
       
def intck_per_row(interval, start_date, end_date):
        if pd.isnull(start_date) or pd.isnull(end_date):
            return np.nan

        interval = interval.lower()

        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, '%Y-%m-%d')

        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, '%Y-%m-%d')

        if interval == 'day':
            return (end_date - start_date).days

        if interval == 'week':
            monday1 = start_date - timedelta(days=start_date.weekday())
            monday2 = end_date - timedelta(days=end_date.weekday())
            return ((monday2 - monday1).dt.days / 7).round().astype(int)

        if interval == 'month':
            start_year, start_month = start_date.year, start_date.month
            end_year, end_month = end_date.year, end_date.month
            return (end_year - start_year) * 12 + (end_month - start_month)

        if interval == 'quarter':
            start_quarter = (start_date.month - 1) // 3 + 1
            end_quarter = (end_date.month - 1) // 3 + 1
            start_year, end_year = start_date.year, end_date.year
            return (end_year - start_year) * 4 + (end_quarter - start_quarter)

        if interval == 'year':
            return end_date.year - start_date.year

        raise ValueError('Invalid interval type. Supported values: day, week, month, quarter, year.')