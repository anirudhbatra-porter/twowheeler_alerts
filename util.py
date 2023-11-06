


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
  return [conn, user, account, warehouse]
