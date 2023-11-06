import pandas as pd
import numpy as np
import io
from google.colab import files
import math
from dateutil import relativedelta
import warnings
import snowflake.connector
warnings.filterwarnings("ignore")
import util

current_user = ''
current_account = ''
current_warehouse = ''
conn = None

def sf_authenticate(user, account, warehouse):
  conn, current_user, current_account, current_warehouse = util.sf_authenticate(user, account, warehouse)

def create_alert(alert_type, alert_name, cron_frequency, cron_expression, email_list, kpi_query=None, validation_query=None, report_table_query=None):
  if alert_type not in ['Validation', 'KPI']:
    return 'Not a valid alert type'
  elif cron_frequency == 'Other (specify CRON expression)' and cron_expression == '':
    return 'Please enter a valid CRON expression'
  elif email_list == '':
    return 'Mailing list is empty'
  elif alert_type == 'Validation' and (validation_query == '' or report_table_query == ''):
    return 'Please enter a valid query'
  elif alert_type == 'KPI' and kpi_query == '':
    return 'Please enter a valid query'
  elif '2_WHEELERS' not in current_warehouse:
    return 'Currently only 2 WHEELER warehouses are supported. For more information, please reach out to anirudh.batra@theporter.in'
try:
  check_table_sql = utils.read_sql_file('check_table.sql')
  check if table is created
  if not, create table

  if cron_frequency is mentioned, assign cron_expression accordingly
  otherwise, validate cron_expression
  try to validate the queries if possible
  When inputs are given and this is submitted, add this entry to a table, execute a query to create the procedure (?) and task, 
  prints a success message and shows the task ID
except(Exception as e):
  print(f"Ran into an exception: {e}")
  
    
