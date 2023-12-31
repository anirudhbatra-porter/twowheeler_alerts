import pandas as pd
import numpy as np
import io
# from google.colab import files
import math
from dateutil import relativedelta
import warnings
import snowflake.connector
from croniter import croniter
from datetime import datetime, timedelta
import constants

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
        # print("Executing fetch_data")
        # print(query)
        df = pd.read_sql(query, session)
        df.columns = df.columns.str.lower()
    except Exception as e:
        print('There was an error with the database (read) operation: \
        {}'.format(e))
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
    syntax_query = "explain using json (" + query + ")"
    output = fetch_data(syntax_query, session)
  except Exception as e:
    print(f"Query validation failed. Please check the syntax. Log: {e}")

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
    # print([validation_flag, fail_reason])
    return [validation_flag, fail_reason] 
  except Exception as e:
    validation_flag = 0
    return [validation_flag, e]
    

def create_sf_task(task_name, procedure_name, alert_name, cron_expression, time_zone, email_list, validation_query, report_table_query, warehouse, session):
  task_query = constants.create_validation_task_query
  char_to_replace = {'TASK_NAME': task_name, 'WAREHOUSE_NAME': warehouse, 'CRON_EXPRESSION': cron_expression, 
                     'TIME_ZONE': time_zone, 'PROCEDURE_NAME': procedure_name, 'query1': validation_query.replace("\n", " ").replace("\t", " ").replace("\b", " ").replace("  ", " "),
                     'query2': report_table_query.replace("\n", " ").replace("\t", " ").replace("\b", " ").replace("  ", " "), 'email_list': email_list, 'alert_name': alert_name}

  for key, value in char_to_replace.items():
    task_query = task_query.replace(key, value)

  t = fetch_data(task_query, session)

def start_sf_task(task_name, session):
  start_query = constants.resume_task_query
  char_to_replace = {'TASK_NAME': task_name}

  for key, value in char_to_replace.items():
    start_query = start_query.replace(key, value)

  s = fetch_data(start_query, session)

def create_db_entry(alert_type, task_name, procedure_name, alert_name, cron_frequency, cron_expression, time_zone, created_by, email_list, validation_query, report_table_query, kpi_query, session):
  insert_db_query = constants.insert_table_query
  char_to_replace = {'CREATED_AT_VALUE': "current_timestamp() + interval '330 minutes'", 'ALERT_TYPE_VALUE': alert_type, 
                     'ALERT_NAME_VALUE': alert_name, 'CRON_FREQUENCY_VALUE': cron_frequency, 'CRON_EXPRESSION_VALUE': cron_expression + f" {time_zone}", 
                     'CREATED_BY_EMAIL_VALUE': created_by, 'MAILING_LIST_VALUE': email_list
                    #  , 'KPI_QUERY_VALUE': kpi_query, 
                    #  'VALIDATION_FIRST_QUERY_VALUE': validation_query.replace("\n", " ").replace("\t", " ").replace("\b", " ").replace("  ", " "), 
                    #  'VALIDATION_SECOND_QUERY_VALUE': report_table_query.replace("\n", " ").replace("\t", " ").replace("\b", " ").replace("  ", " ")
                    }

  for key, value in char_to_replace.items():
    insert_db_query = insert_db_query.replace(key, value)

  i = fetch_data(insert_db_query, session)

def fetch_alerts(created_by_email, session, fetch_all=0):
    show_alerts_query = constants.fetch_alerts_query
    if fetch_all == 0:
      char_to_replace = {'CREATED_BY_EMAIL_VALUE': created_by_email}
    else:
      char_to_replace = {"WHERE CREATED_BY_EMAIL = 'CREATED_BY_EMAIL_VALUE'": ""}
      

    for key, value in char_to_replace.items():
      show_alerts_query = show_alerts_query.replace(key, value)
    
    alerts_df = fetch_data(show_alerts_query, session)

    if alerts_df.shape[0] == 0:
      raise Exception(f"No alerts created with this email were found")
    
    return alerts_df

def delete_sf_procedures(procedure_list, session):
    prefix = "SANDBOX_DB.TWO_WHEELERS."

    for procedure in procedure_list:
      p = fetch_data(f"DROP PROCEDURE {prefix+procedure}(string, string, string, string)", session)

def delete_sf_tasks(task_list, session):
    prefix = "SANDBOX_DB.TWO_WHEELERS."

    for task in task_list:
      t = fetch_data(f"DROP TASK {prefix+task}", session)

def delete_db_entries(alert_list, session):
    d = fetch_data(f"DELETE FROM SANDBOX_DB.TWO_WHEELERS.SCHEDULED_ALERTS WHERE ALERT_ID IN {tuple(alert_list)}".replace(",)", ")"), session)
