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

  data['id'],
	data['number'], 
	data['description']
	data['type'], 
	data['procedure_place'], 
	data['trade_type'], 
	data['federal_law'],
        data['organizer']['name'],
	data['organizer']['inn'],
	data['organizer']['kpp'], 
	data['organizer']['country'],
        data['customer']['name'], 
	data['customer']['inn'], 
	data['customer']['kpp'], 
	data['customer']['country'],
        data['status'], 
	data['date_begin'], 
	data['date_end'], 
	data['date_trade_end'], 
	data['payment_terms'], 
	data['currency'],
	data['alternate_offer'],
        data['url'],
        str (data['lots'][0]['id']),
	data['lots'[0]['name']]
        data['lots'][0]['description'],
        data['lots'][0]['delivery_place'], 
	data['lots'][0]['quantity'],
	data['lots'][0]['unit_name'], 
	data['lots'][0]['price'],
        data['lots'][0]['price_no_tax'], 
	data['os_number'], 
	data['os_name'], 
	data['publish_date'], 
	data['change_date'], 
	data['comment'], 
	data['delivery_address']
