import pandas as pd
import numpy as np
import io
from google.colab import files
import math
from dateutil import relativedelta
import warnings
import snowflake.connector

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
