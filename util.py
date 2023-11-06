import pandas as pd
import numpy as np
import io
from google.colab import files
import math
from dateutil import relativedelta
import warnings
import snowflake.connector
from croniter import croniter

def sf_authenticate(user, account, warehouse):
  conn = snowflake.connector.connect(
    user = user,
    account = account,
    authenticator = "externalbrowser",
    warehouse = warehouse
  )
  return [conn, user, account, warehouse]

def read_sql_file(file_name):
  fd = open(file_name, 'r')
  sqlFile = fd.read()
  fd.close()
  return sqlFile

def fetch_data(query, session):
    df = pd.DataFrame()
    try:
        df = session.sql(query)
        df = df.to_pandas()
        df.columns = df.columns.str.lower()
    except Exception as e:
        print('There was an error with the database (read) operation: \
        {}'.format(error))
        print(query)
    return df
            
def write_to_sfdb(df_dict, session, overwrite_dict):
    try:
        
        for table_key, df_val in df_dict.items():
            df_val.columns = df_val.columns.str.upper()
            if df_val.shape[0] == 0:
                print("Dataframe for table: " + str(table_key) + "is empty. Skipping the write operation.")
                continue
            # using the snowflake session object to write pandas dataframe to table in snowflake
            session.write_pandas(df=df_val, table_name=table_key, overwrite=overwrite_dict[table_key], database='SANDBOX_DB', schema='TWO_WHEELERS', auto_create_table=True)
            print("Data has been written into table: " + str(table_key))
    
    except Exception as e:
            print('There was an error with the database (write) operation: \
                {}'.format(e))

def validate_cron_expression(exp):
  if 'L' not in exp:
    return croniter.is_valid(exp)
  return True
