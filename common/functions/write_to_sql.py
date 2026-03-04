
import urllib
from sqlalchemy import create_engine

def get_engine(server="localhost", database="Library", driver="ODBC Driver 17 for SQL Server"):
    params = urllib.parse.quote_plus(
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        "Trusted_Connection=yes;"
        "TrustServerCertificate=yes;"
    )

    return create_engine(
        f"mssql+pyodbc:///?odbc_connect={params}",
        fast_executemany=True
    )

def write_to_sql(df, table_name, db_schema, if_exists="replace", engine=None):

    # allow dependency injection for testing
    engine = engine or get_engine()

    rows_written = df.to_sql(
        name=table_name,
        con=engine,
        schema=db_schema,
        if_exists=if_exists,
        index=False,
        chunksize=5000,
        method="multi"
    )

    return rows_written