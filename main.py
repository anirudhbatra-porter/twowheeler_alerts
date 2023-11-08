import pandas as pd
import numpy as np
import io
from google.colab import files
import math
from dateutil import relativedelta
import warnings
import snowflake.connector
warnings.filterwarnings("ignore")
import util, constants

current_user = ''
current_account = ''
current_warehouse = ''
conn = None
procedure_name = ''
task_name = ''

def sf_authenticate(user, account, warehouse):
  conn, current_user, current_account, current_warehouse = util.sf_authenticate(user, account, warehouse)

def create_validation_alert(alert_name, cron_frequency, cron_expression, email_list, validation_query, report_table_query):
  procedure_name = str.upper(alert_name) + '_PROCEDURE'
  task_name = str.upper(alert_name) + '_TASK'
  p = utils.fetch_data(constants.create_validation_procedure_query.replace('PROCEDURE_NAME', procedure_name))
  # create these functions
  utils.create_sf_task(task_name, procedure_name, alert_name, cron_expression, constants.time_zone, email_list, validation_query, report_table_query, current_warehouse, conn)
  utils.create_db_entry('Validation Alert', task_name, procedure_name, alert_name, cron_frequnecy, cron_expression, time_zone, current_user, email_list, validation_query, report_table_query, '', conn)

def create_kpi_alert(alert_name, cron_frequency, cron_expression, email_list, kpi_query):
  print(constants.capability_unavailable_msg)

def create_alert(alert_type, alert_name, cron_frequency, cron_expression, email_list, kpi_query=None, validation_query=None, report_table_query=None):
  try:
    validation_flag, fail_reason = utils.validate_alert(alert_type, cron_frequency, cron_expression, email_list, validation_query, report_table_query, kpi_query, current_warehouse, conn)
    
    if ~validation_flag:
      Raise Exception(f"Validation failed with reason: {fail_reason}")
      
    check_table_sql = utils.read_sql_file('check_table.sql')
    check_table_flag = utils.fetch_data(check_table_sql, conn).flag.iloc[0]
  
    if ~check_table_flag:
      create_table_sql = utils.read_sql_file('create_alerts_table.sql')
      create_table_result = utils.fetch_data(create_table_sql, conn)
  
    if cron_frequency <> 'Other (specify CRON expression)' and cron_expression == '':
      cron_expression = constants.cron_map[cron_frequency]
  
    if ~utils.validate_cron_expression(cron_expression):
      return 'Please enter a valid CRON expression'
  
    if alert_type == 'Validation':
      create_validation_alert(alert_name, cron_frequency, cron_expression, email_list, validation_query, report_table_query)
    else:
      create_kpi_alert(alert_name, cron_frequency, cron_expression, email_list, kpi_query)
  
  except(Exception as e):
    print(f"Ran into an exception: {e}")
  
    
