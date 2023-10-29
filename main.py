import pandas as pd
import numpy as np
import io
from google.colab import files
import math
from dateutil import relativedelta
import warnings
warnings.filterwarnings("ignore")
import snowflake.connector


def sf_authenticate(user, account, warehouse):
  conn = snowflake.connector.connect(
    user="anirudh.batra@theporter.in",
    account="ss29587.ap-southeast-1",
    authenticator="externalbrowser",
    warehouse="WH_PROD_2_WHEELERS_XS"
  )

def create_alert():
  print('hello!')
