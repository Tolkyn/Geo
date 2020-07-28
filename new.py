from datetime import datetime, timedelta
import airflow
from airflow import DAG
from airflow.operators.python_operators import PythonOperator
import requests
import pyodbc
defult_args= {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019,10,1),
    'email': ['airflow@example.com'],
    'retries':2
}

dag=DAG(
    'Perfect', defult_args=defult_args,
    description='Code for ETS',
    schedule_interval=timedelta(days=1)
)
task_1= PythonOperator(task_id='First_function', provide_context=True, python_callable= First_function, dag=dag)
task_2= PythonOperator(task_id='Second_function', provide_context=True, python_callable= Second_function, dag=dag)
task_1>>task_2

