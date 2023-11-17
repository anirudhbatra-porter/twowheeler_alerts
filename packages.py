import subprocess

packages_to_install = ['snowflake-connector-python==2.8.3', 'pandas', 'numpy', 'math', 'dateutil', 'warnings', 'croniter', 'datetime', 'io']

def install_dependencies():
  for package in packages_to_install:
    subprocess.run(['pip', 'install', package])
