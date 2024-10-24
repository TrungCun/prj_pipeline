import logging
import sql_statements
import datetime

from airflow import DAG

dag = DAG(
  'ny_taxi',
  description = 'pipeline process data ny taxi',
  start_date = datetime.datetime.now(),
  schedule_interval = '@weekly',
  tags = ['ny']
)

def loading_table(table):
  redshift_hook = PostgresHook("redshift")

  if table == 'dim_vendor':
    sql_stmt = sql_statements.load_dim_vendor
  elif table == 'dim_storefwd':
    sql_stmt = sql_statements.load_dim_storefwd
  elif table == 'dim_ratecode':
    sql_stmt = sql_statements.load_dim_ratecode
  elif table == 'dim_payment':
    sql_stmt = sql_statements.load_dim_payment
  elif table == 'fact_':
    sql_stmt = sql_statements.load
  else:


  redshift_hook.run(sql_stmt)
  print(f"Table {table} was loaded successfully.")

def data_quality_checks(tables):
  tables = tables.split(',')
  redshift_hook = PostgresHook("redshift")
  for table in tables:
    records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
    if len(records) < 1 or len(records[0]) < 1:
      raise ValueError(f"Data quality check failed. {table} returned no results")
    num_records = records[0][0]
    if num_records < 1:
      raise ValueError(f"Data quality check failed. {table} contained 0 rows")
    logging.info(f"Data quality on table {table} check passed with {records[0][0]} records")

# <-------------------DATA QUALITY CHECKS------------------->

data_quality_checks_task = PythonOperator(
  task_id = 'data_quality_checks',
  dag = dag,
  python_callable = data_quality_checks,
  op_kwargs = {
    'tables' : 'fact_yellow, fact_zones',
  }
)

# <-------------------LOADING TABLES------------------->

loading_dim_vendor_task = PythonOperator(
  task_id = 'loading_dim_vendor',
  dag = dag,
  op_kwargs = {'table': 'dim_vendor'},
  python_callable = loading_table,
)

loading_dim_storefwd_task = PythonOperator(
  task_id = 'loading_dim_storefwd',
  dag = dag,
  op_kwargs = {'table': 'dim_storefwd'},
  python_callable = loading_table,
)

loading_dim_ratecode_task = PythonOperator(
  task_id = 'loading_dim_ratecode',
  dag = dag,
  op_kwargs = {'table': 'dim_ratecoded'},
  python_callable = loading_table,
)

loading_dim_payment_task = PythonOperator(
  task_id = 'loading_dim_payment',
  dag = dag,
  op_kwargs = {'table': 'dim_payment'},
  python_callable = loading_table,
)

# <-------------------CREATING TABLES------------------->
