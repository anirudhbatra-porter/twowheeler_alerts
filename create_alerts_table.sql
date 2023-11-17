create or replace TABLE SANDBOX_DB.TWO_WHEELERS.SCHEDULED_ALERTS (
  ALERT_ID INTEGER AUTOINCREMENT START 1 INCREMENT 1,
  CREATED_AT TIMESTAMP,
  ALERT_TYPE VARCHAR(16777216),
  ALERT_NAME VARCHAR(16777216),
  CRON_FREQUENCY VARCHAR(16777216),
  CRON_EXPRESSION VARCHAR(16777216),
  CREATED_BY_EMAIL VARCHAR(16777216),
  MAILING_LIST VARCHAR(16777216)
  );
