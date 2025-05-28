# streamlit_app/snowflake_connector.py

import snowflake.connector
import pandas as pd

def get_snowflake_connection():
    return snowflake.connector.connect(
        user='dataexpert_student',
        password='DataExpert123!',
        account='aab46027',
        warehouse='COMPUTE_WH',
        database='dataexpert_student',
        schema='BENCEKOVACS'
    )

def run_query(query):
    conn = get_snowflake_connection()
    cs = conn.cursor()
    try:
        cs.execute(query)
        df = cs.fetch_pandas_all()
    finally:
        cs.close()
    return df
