import pandas as pd
import numpy as np
import io
from google.colab import files
import math
from dateutil import relativedelta
import warnings
warnings.filterwarnings("ignore")
import snowflake.connector

current_user = ''
current_account = ''
current_warehouse = ''
conn = None

def sf_authenticate(user, account, warehouse):
  current_user = user
  current_account = account
  current_warehouse = warehouse
  conn = snowflake.connector.connect(
    user = user,
    account = account,
    authenticator = "externalbrowser",
    warehouse = warehouse
  )

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

except(Exception as e):
  print(f"Ran into an exception: {e}")
  
    
