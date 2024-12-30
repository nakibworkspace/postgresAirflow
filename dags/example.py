# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.providers.postgres.operators.postgres import PostgresOperator
# from airflow.providers.postgres.hooks.postgres import PostgresHook
# import pandas as pd
# from datetime import datetime, timedelta

# # Default argument for the DAG
# default_args= {
#     'owner':'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2024,1,1),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries' : 1,
#     'retry_delay': timedelta(minutes=5)
# }

# #initializing the data
# dag= DAG(
#     'postgres_etl_example',
#     default_args=default_args,
#     description= 'Example postgres ETL example',
#     schedule_interval=timedelta(days=1),
#     catchup=False
# )

# #create table
# create_table_sql= """
# CREATE TABLE IF NOT EXISTS sales (
#     id SERIAL PRIMARY KEY,
#     date DATE,
#     product_id INTEGER,
#     quantity INTEGER,
#     revenue DECIMAL(10,2)
# );
# """

# create_table= PostgresOperator(
#     task_id= 'create_table',
#     postgres_conn_id='default_postgres',
#     sql= create_table_sql,
#     dag= dag
# )

# #function to process and transform data
# def process_data(**context):
#     # Initialize postgreshook
#     pg_hook= PostgresHook(postgres_conn_id='postgres_default')

#     #Example: Query data from source table
#     source_data= pg_hook.get_pandas_df(
#         """
#         SELECT * FROM source_sales
#         WHERE date = %s
#         """,
#         parameters=[context['execution_date'].strftime('%Y-%m-%d')]

#     )
#     # perform transformation
#     transformed_data= source_data.copy()
#     transformed_data['revenue']= transformed_data['quantity'] * transformed_data['unit_price']

#     #Save transformed data back to postgres
#     transformed_data.to_sql(
#         'sales',
#         pg_hook.get_sqlalchemy_engine(),
#         if_exists='append',
#         index=False
#     )

#     return f"Processed {len(transformed_data)} rows"
# #Process data task
# process_data_task= PythonOperator(
#     task_id= 'process_data',
#     python_callable=process_data,
#     provide_context= True,
#     dag=dag
#     )

# #Validate data tasks
# validate_data_sql= """
# SELECT 
#     date,
#     COUNT(*) as record_count,
#     SUM(quantity) as total_quantity,
#     SUM(revenue) as total_revenue
# FROM sales
# WHERE date = '{{ ds }}'
# GROUP BY date;
# """

# validate_data= PostgresOperator(
#     task_id= 'validate_data',
#     postgres_conn_id='postgres_dafault',
#     sql=validate_data_sql,
#     dag=dag
# )

# # define task dependencies
# create_table >> process_data_task >> validate_data