import pandas as pd
import numpy as np
import io
# from google.colab import files
import math
from dateutil import relativedelta
import warnings
import snowflake.connector
warnings.filterwarnings("ignore")
import util, constants

global current_user, current_account, current_warehouse, conn, procedure_name, task_name
current_user = ''
current_account = ''
current_warehouse = ''
conn = None
procedure_name = ''
task_name = ''

def sf_authenticate(user, account, warehouse):
  global conn, current_user, current_account, current_warehouse
  conn, current_user, current_account, current_warehouse = util.sf_authenticate(user, account, warehouse)

def create_validation_alert(alert_name, cron_frequency, cron_expression, email_list, validation_query, report_table_query):
  global procedure_name, task_name, conn
  procedure_name = str.upper(alert_name) + '_PROCEDURE'
  task_name = str.upper(alert_name) + '_TASK'
  p = util.fetch_data(constants.create_validation_procedure_query.replace('PROCEDURE_NAME', procedure_name), conn)
  print(f"Snowflake procedure has been created successfully: {procedure_name}")

  util.create_sf_task(task_name, procedure_name, alert_name, cron_expression, constants.time_zone, email_list, validation_query, report_table_query, current_warehouse, conn)
  print(f"Snowflake task has been created successfully: {task_name}")
  util.start_sf_task(task_name, conn)
  print(f"Snowflake task has been started/resumed successfully")
  util.create_db_entry('Validation Alert', task_name, procedure_name, alert_name, cron_frequency, cron_expression, constants.time_zone, current_user, email_list, validation_query, report_table_query, '', conn)
  print(f"DB entry has been created successfully")
  last_insert_query = util.read_sql_file('last_inserted_row.sql')
  last_inserted = util.fetch_data(last_insert_query, conn)

  if last_inserted.shape[0] > 0:
    print(f"""Validation Alert has been created successfully. \nAlert ID: {last_inserted[last_inserted.columns[0]].iloc[0]}
    """)
  #  \nSF Task Name: {task_name}\nSF Procedure Name: {procedure_name}

def create_kpi_alert(alert_name, cron_frequency, cron_expression, email_list, kpi_query):
  print(constants.capability_unavailable_msg)

def create_alert(alert_type, alert_name, cron_frequency, cron_expression, email_list, kpi_query=None, validation_query=None, report_table_query=None):
  try:
    # print("Trying to validation the information provided..")
    validation_flag, fail_reason = util.validate_alert(alert_type, cron_frequency, cron_expression, email_list, validation_query, report_table_query, kpi_query, current_warehouse, conn)

    if validation_flag == 0:
      raise Exception(f"Validation failed with reason: {fail_reason}")
      
    check_table_sql = util.read_sql_file('check_table.sql')
    check_table_flag = util.fetch_data(check_table_sql, conn).flag.iloc[0]
  
    if check_table_flag == False:
      create_table_sql = util.read_sql_file('create_alerts_table.sql')
      create_table_result = util.fetch_data(create_table_sql, conn)
      print("Scheduled Alerts table has been created..")
  
    if cron_frequency != 'Other (specify CRON expression)' and cron_expression == '':
      cron_expression = constants.cron_map[cron_frequency]
  
    if util.validate_cron_expression(cron_expression) == False:
      return 'Please enter a valid CRON expression'

    print("Validation completed..")
    if alert_type == 'Validation':
      create_validation_alert(alert_name, cron_frequency, cron_expression, email_list, validation_query, report_table_query)
    else:
      create_kpi_alert(alert_name, cron_frequency, cron_expression, email_list, kpi_query)
  
  except Exception as e:
    print(f"Ran into an exception: {e}")


def show_all_alerts():
  try:
    return util.fetch_alerts("", conn, fetch_all=1)
  except Exception as e:
    print(f"Ran into an exception: {e}")

def show_alerts(created_by_email):
  try:
    return util.fetch_alerts(created_by_email, conn)
  except Exception as e:
    print(f"Ran into an exception: {e}")

def delete_alert(alert_id):
  try:
    alerts_df = util.fetch_alerts("", conn, fetch_all=1)
    alert = alerts_df[alerts_df.alert_id == alert_id]

    if alert.shape[0] == 0:
      raise Exception(f"Alert with this ID doesn't exist")
    
    procedure_name = str.upper(alert.iloc[0].alert_name) + '_PROCEDURE'
    task_name = str.upper(alert.iloc[0].alert_name) + '_TASK'
    alert_id = alert.iloc[0].alert_id

    util.delete_sf_procedures([procedure_name], conn)
    util.delete_sf_tasks([task_name], conn)
    util.delete_db_entries([alert_id], conn)
    print("Alert has been deleted successfully")
  except Exception as e:
    print(f"Ran into an exception: {e}")

def delete_all_alerts():
  try:
    alerts_df = util.fetch_alerts("", conn, fetch_all=1)

    if alerts_df.shape[0] == 0:
      raise Exception(f"No alerts found")
    
    alerts_df['procedure_name'] = alerts_df.alert_name.apply(lambda alert: str.upper(alert) + '_PROCEDURE')
    alerts_df['task_name'] = alerts_df.alert_name.apply(lambda alert: str.upper(alert) + '_TASK')

    util.delete_sf_procedures(alerts_df.procedure_name.unique(), conn)
    util.delete_sf_tasks(alerts_df.task_name.unique(), conn)
    util.delete_db_entries(alerts_df.alert_id.unique(), conn)
    print("All alerts have been deleted successfully")
  except Exception as e:
    print(f"Ran into an exception: {e}")
