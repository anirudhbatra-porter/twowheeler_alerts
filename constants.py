cron_map = {
  'Daily at 7AM': '0 7 * * *',
  'Daily at 10AM': '0 10 * * *',
  'Daily at 7PM': '0 19 * * *',
  'Weekly on Mondays at 10AM': '0 10 * * MON',
  'Weekly on Fridays at 10AM': '0 10 * * FRI'
}

capability_unavailable_msg = 'This capability is coming soon! For more information, please reach out to anirudh.batra@theporter.in'

time_zone = 'Asia/Kolkata'

create_validation_procedure_query = '''
CREATE OR REPLACE PROCEDURE SANDBOX_DB.TWO_WHEELERS.PROCEDURE_NAME(query1 string, query2 string, email_list string, alert_name string)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python', 'tabulate')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

# Snowpark
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col
from snowflake.snowpark.exceptions import SnowparkSQLException
# Libraries
import time
from datetime import datetime, timedelta
import yaml
from pandas.tseries.offsets import MonthEnd
# import matplotlib as plt
# import seaborn as sns
# Dataframe manipulation
import pandas as pd 
import pandas.io.sql as sqlio
import numpy as np
# Math
import math

def fetch_data(query, session):
    df = pd.DataFrame()
    try:
        df = session.sql(query)
        df = df.to_pandas()
        df.columns = df.columns.str.lower()
    except SnowparkSQLException as error:
        print("There was an error with the database operation: {}".format(error))
        print(query)
    return df

def send_email(session, email_list, alert_name, result1, result2, success_flag=0):
  if success_flag == 0:
    session.call("system$send_email",
        "email_integration_two_wheelers",
        email_list,
        f"Validation Alert: {alert_name} has been triggered",
        f"""There has been a mismatch between the results from the source queries provided while creating this alert. \nSource 1: \n{result1} \n\nSource 2: \n{result2}""")
  else:
    session.call("system$send_email",
        "email_integration_two_wheelers",
        email_list,
        f"Validation Alert: {alert_name} ran successfully",
        f"""There was no mismatch between the results from the source queries provided while creating this alert""")

def main(session: snowpark.Session, query1, query2, email_list, alert_name): 
    result1 = fetch_data(query1, session)
    result2 = fetch_data(query2, session)

    validation_flag = True

    if result1.shape != result2.shape:
      validation_flag = False

    result2.columns = result1.columns

    if result1.equals(result2)==False:
      validation_flag = False

    if validation_flag == False:
      send_email(session, email_list, alert_name, result1.to_markdown(), result2.to_markdown())
    else:
      send_email(session, email_list, alert_name, result1.to_markdown(), result2.to_markdown(), 1)
      
    return "REACHED END OF CODE"
    ';
'''

create_validation_task_query = """
CREATE OR REPLACE TASK SANDBOX_DB.TWO_WHEELERS.TASK_NAME
WAREHOUSE = WAREHOUSE_NAME
SCHEDULE = 'USING CRON CRON_EXPRESSION TIME_ZONE'
AS
CALL SANDBOX_DB.TWO_WHEELERS.PROCEDURE_NAME($$query1$$, $$query2$$, 'email_list', 'alert_name')
"""

insert_table_query = """
INSERT INTO SANDBOX_DB.TWO_WHEELERS.SCHEDULED_ALERTS (CREATED_AT, ALERT_TYPE, ALERT_NAME, CRON_FREQUENCY, CRON_EXPRESSION, CREATED_BY_EMAIL, MAILING_LIST)
VALUES (CREATED_AT_VALUE, 'ALERT_TYPE_VALUE', 'ALERT_NAME_VALUE', 'CRON_FREQUENCY_VALUE', 'CRON_EXPRESSION_VALUE', 'CREATED_BY_EMAIL_VALUE', 'MAILING_LIST_VALUE'
);
"""

# , KPI_QUERY, VALIDATION_FIRST_QUERY, VALIDATION_SECOND_QUERY
# , "KPI_QUERY_VALUE", "VALIDATION_FIRST_QUERY_VALUE", "VALIDATION_SECOND_QUERY_VALUE"


fetch_alerts_query = """
SELECT * FROM SANDBOX_DB.TWO_WHEELERS.SCHEDULED_ALERTS WHERE CREATED_BY_EMAIL = 'CREATED_BY_EMAIL_VALUE'
"""

resume_task_query = """
ALTER TASK SANDBOX_DB.TWO_WHEELERS.TASK_NAME RESUME;
"""
