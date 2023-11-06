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

def validate_query(query, session):
  try:
    syntax_query = 'explain using json (' + query + ')'
    output = fetch_date(syntax_query, session)
  except Exception as e:
    print('Query validation failed. Please check the syntax')

def validate_alert(alert_type, cron_frequency, cron_expression, email_list, validation_query, report_table_query, kpi_query, current_warehouse, session):
  fail_reason = ''
  validation_flag = 1

  try:      
    if alert_type not in ['Validation', 'KPI']:
      fail_reason = 'Not a valid alert type'
    elif cron_frequency == 'Other (specify CRON expression)' and cron_expression == '':
      fail_reason = 'Please enter a valid CRON expression'
    elif email_list == '':
      fail_reason = 'Mailing list is empty'
    elif alert_type == 'Validation' and (validation_query == '' or report_table_query == ''):
      fail_reason = 'Please enter a valid query'
    elif alert_type == 'KPI' and kpi_query == '':
      fail_reason = 'Please enter a valid query'
    elif '2_WHEELERS' not in current_warehouse:
      fail_reason = 'Currently only 2 WHEELER warehouses are supported. For more information, please reach out to anirudh.batra@theporter.in'
  
    if fail_reason != '':
      validation_flag = 0
      
    if alert_type == 'Validation':
      validate_query(validation_query, session)
      validate_query(report_table_query, session)
    else:
      validate_query(kpi_query, session)
      
    return [validation_flag, fail_reason] 
  except Exception as e:
    validation_flag = 0
    return [validation_flag, e]
