import requests
import pyodbc
import datetime
from datetime import datetime 
from datetime import timedelta
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator



default_args= {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019,10,1),
    'email': ['airflow@example.com'],
    'retries':2}


dag = DAG('ETS_Tenders',
    default_args=default_args,
    description='This DAG will help u to find errors',
    schedule_interval=timedelta(days=1)
    )

def Token(**contex):  
    URL_1 = 'https://www.ets-tender.kz/integration/json/User.Login/?login=galym.b@geometry.kz&password=Geometry987'
    headers_1 = {'Content-Type': 'application/x-www-form-urlencoded'}
    r = requests.post(URL_1, headers=headers_1)
    rr = r.json()
    token = rr['access_token']
    tokens = ('Bearer' + ' ' + rr['access_token'])
    
    contex['ti'].xcom_push(key='SendToken', value=token)
    contex['ti'].xcom_push(key='SendTokens', value=tokens)
        
    return token ,tokens


def Getting_ID(**contex):
    
    token=contex['ti'].xcom_pull(key='SendToken')
    tokens=contex['ti'].xcom_pull(key='SendTokens')
    
    now = datetime.now()
    now =  int(now.timestamp())
    to = 0
    while to<=now:
        for i in range(4):
            d_from =datetime(2020, 7, 2) + i*timedelta(days=7)
            d_to = d_from + timedelta(days=7)
            to = int(d_to.timestamp())
            from_=int(d_from.timestamp())
            URL_2 = 'https://www.ets-tender.kz/integration/json/TradeProcedures.GetList?access_token=' + str(token) + '&date_from=' + str(from_) + '&date_to=' + str(to)
            headers_2 = {'Authorization': 'Bearer  $ACCESS_TOKEN'}
            headers_2['Authorization'] = tokens
            r_2 = requests.get(URL_2, headers=headers_2).json()
            r_2 = r_2['trade_list']
            print(len(r_2))
            sett = []
            for i in r_2:
                ID = i['id']
                sett.append(ID)
      
            for j in sett:
   
            	URL_3 = 'https://www.ets-tender.kz/integration/json/TradeProcedures.GetShortTrade?access_token=' + str(token) + '&id=' + str(j)
            	r_3 = r_3 = requests.get(URL_3, headers=headers_2).json()
            	data = r_3['short_trade_procedure']
            	print(data)
            	dsn = 'sqlserverdatasource'
		user = 'sa'
		password = 'P@ssw0rd@2020'
		database = 'expDB'
		con_string = 'DSN=%s;UID=%s;PWD=%s;DATABASE=%s;'% (dsn, user, password, database)
		cnxn = pyodbc.connect(con_string) 
		
		query = 'insert into Tender(id, number, _description, _type, procedure_place, trade_type, federal_law, organizer_name, organizer_iin, organizer_kpp, organizer_country, customer_name, customer_iin, customer_kpp, customer_country, _status, date_begin, date_end, date_trade_end, payment_terms, currency, alternate_offer, _url, lots_id, lots_description, lots_delivery_place, lots_quantity, lots_unit_name, lots_price, lots_price_no_tax, positions,os_number, os_name, publish_date, change_date, comment, delivery_address) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
		args = (data['id'], data['number'], data['description'], data['type'], data['procedure_place'], data['trade_type'], data['federal_law'], data['organizer']['name'],
	data['organizer']['inn'], data['organizer']['kpp'], data['organizer']['country'], data['customer']['name'], data['customer']['inn'], data['customer']['kpp'], data['customer']['country'],
        data['status'], data['date_begin'], data['date_end'], data['date_trade_end'], data['payment_terms'], data['currency'], data['alternate_offer'], data['url'], str (data['lots'][0]['id']),
	data['lots'][0]['name'], data['lots'][0]['description'], data['lots'][0]['delivery_place'], data['lots'][0]['quantity'], data['lots'][0]['unit_name'], data['lots'][0]['price'],
        data['lots'][0]['price_no_tax'], data['os_number'], data['os_name'], data['publish_date'], data['change_date'], data['comment'], data['delivery_address'])
              
                

task_1=PythonOperator(task_id='Token', provide_context=True, python_callable= Token, dag=dag)
task_2= PythonOperator(task_id='ID', provide_context=True, python_callable= Getting_ID, dag=dag)

task_1>>task_2
print("the end programm")
