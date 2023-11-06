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

def sf_authenticate(user, account, warehouse):
  conn, current_user, current_account, current_warehouse = util.sf_authenticate(user, account, warehouse)

def create_alert(alert_type, alert_name, cron_frequency, cron_expression, email_list, kpi_query=None, validation_query=None, report_table_query=None):
try:
  validation_flag, fail_reason = utils.validate_alert(alert_type, cron_frequency, cron_expression, email_list, validation_query, report_table_query, kpi_query, current_warehouse)
  
  if !validation_flag:
    Raise Exception(f"Validation failed with reason: {fail_reason}")
    
  check_table_sql = utils.read_sql_file('check_table.sql')
  check_table_flag = utils.fetch_data(check_table_sql, conn).flag.iloc[0]

  if check_table_flag == False:
    create_table_sql = utils.read_sql_file('create_alerts_table.sql')
    create_table_result = utils.fetch_data(create_table_sql, conn)

  if cron_frequency <> 'Other (specify CRON expression)' and cron_expression == '':
    cron_expression = constants.cron_map[cron_frequency]

  if utils.validate_cron_expression(cron_expression) == False:
    return 'Please enter a valid CRON expression'

  utils.validate_query()
  try to validate the queries if possible
  When inputs are given and this is submitted, add this entry to a table, execute a query to create the procedure (?) and task, 
  prints a success message and shows the task ID
except(Exception as e):
  print(f"Ran into an exception: {e}")
  
    
