import requests
import pyodbc
import datetime
from datetime import datetime, timedelta
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

#----------------------------------------------------------------------------------------------------


default_args ={'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020,7,28, 00, 00),
    'retries':1}
    
dag = DAG('Refreshing_data_weekly',
    default_args=default_args,
    description='This DAG will help u to find errors', catchup=False, 
    schedule_interval='0 0 * * 3')   
    
dsn = 'sqlserverdatasource'
user='sa'
password = 'P@ssw0rd@2020'
database = 'GS'
con_string = 'DSN=%s;UID=%s;PWD=%s;DATABASE=%s;'% (dsn, user, password, database)
cnxn = pyodbc.connect(con_string)


#------------------------------------------------------------------------------------------------------
   
def ref_buy_lot_reject_reason_1(**context):
    URL_n = 'https://ows.goszakup.gov.kz/v3/refs/ref_buy_lot_reject_reason'
    token = 'Bearer' + ' ' + 'f309ecc15d574833c45713da702049e2'
    headers_1 = { 'Content-Type' : 'application/json','Authorization':'none' }
    headers_1['Authorization'] = token
    next_page = 'none'
    
    cnxn.execute('TRUNCATE TABLE gs.dbo.ref_buy_lot_reject_reason')
    
    while next_page != '':
        r_n = requests.get(URL_n, headers=headers_1, verify=False).json()
        next_page = r_n['next_page']
        item = r_n['items']
        for i in item:
            query = 'insert into gs.dbo.ref_buy_lot_reject_reason( item_id, item_name_ru , item_name_kz) values (?,?,?)'
            args = (i['id'], i['name_ru'], i['name_kz'])
            cnxn.execute(query, args)
            cnxn.commit()
            URL_n = 'https://ows.goszakup.gov.kz' + next_page    

#---------------------------------------------------------------------------------------    
def ref_reason_2(**context):
    token = ('Bearer' + ' ' + 'f309ecc15d574833c45713da702049e2')
    headers_1 = { 'Content-Type' : 'application/json','Authorization':'none' }
    headers_1['Authorization'] = token
    next_page = 'none'
    URL_n = 'https://ows.goszakup.gov.kz/v3/refs/ref_reason'
    
    cnxn.execute('TRUNCATE TABLE gs.dbo.ref_reason')
    
    while next_page != '':
        r_n = requests.get(URL_n, headers=headers_1, verify=False).json()
        print(r_n)
        next_page = r_n['next_page']
        item = r_n['items']
        for i in item:
            query = 'insert into gs.dbo.ref_reason( item_id, item_type, item_name_ru , item_name_kz) values (?,?,?,?)'
            args = (i['id'], i['type'], i['name_ru'], i['name_kz'])
            cnxn.execute(query, args)
            cnxn.commit()
        URL_n = 'https://ows.goszakup.gov.kz' + next_page
#---------------------------------------------------------------------------------------       
def ref_contract_type_3(**context):
    token = ('Bearer' + ' ' + 'f309ecc15d574833c45713da702049e2')
    headers_1 = { 'Content-Type' : 'application/json','Authorization':'none' }
    headers_1['Authorization'] = token
    
    cnxn.execute('TRUNCATE TABLE gs.dbo.ref_contract_type')
    
    next_page = 'none'
    URL_n = 'https://ows.goszakup.gov.kz/v3/refs/ref_contract_type'
    while next_page != '':
        r_n = requests.get(URL_n, headers=headers_1, verify=False).json()
        print(r_n)
        next_page = r_n['next_page']
        item = r_n['items']
        for i in item:
            query = 'insert into gs.dbo.ref_contract_type( item_id, item_name_ru , item_name_kz) values (?,?,?)'
            args = (i['id'], i['name_ru'], i['name_kz'])
            cnxn.execute(query, args)
            cnxn.commit()
        URL_n = 'https://ows.goszakup.gov.kz' + next_page
#--------------------------------------------------------------------------------------	
def ref_contract_cancel_4(**context):
    token = ('Bearer' + ' ' + 'f309ecc15d574833c45713da702049e2')
    headers_1 = { 'Content-Type' : 'application/json','Authorization':'none' }
    headers_1['Authorization'] = token
    next_page = 'none'
    URL_n = 'https://ows.goszakup.gov.kz/v3/refs/ref_contract_cancel'
    
    cnxn.execute('TRUNCATE TABLE gs.dbo.ref_contract_cancel')
    
    while next_page != '':
        r_n = requests.get(URL_n, headers=headers_1, verify=False).json()
        print(r_n)
        next_page = r_n['next_page']
        item = r_n['items']
        for i in item:
            query = 'insert into gs.dbo.ref_contract_cancel( item_id, item_name_ru , item_name_kz) values (?,?,?)'
            args = (i['id'], i['name_ru'], i['name_kz'])
            cnxn.execute(query, args)
            cnxn.commit()
        URL_n = 'https://ows.goszakup.gov.kz' + next_page
#--------------------------------------------------------------------------------------
def ref_currency_5(**context):
    token = ('Bearer' + ' ' + 'f309ecc15d574833c45713da702049e2')
    headers_1 = { 'Content-Type' : 'application/json','Authorization':'none' }
    headers_1['Authorization'] = token
    next_page = 'none'
    URL_n = 'https://ows.goszakup.gov.kz/v3/refs/ref_currency?limit=200'
    
    cnxn.execute('TRUNCATE TABLE gs.dbo.ref_currency')
    
    while next_page != '':
        r_n = requests.get(URL_n, headers=headers_1, verify=False).json()
        next_page = r_n['next_page']
        item = r_n['items']
        print("len items:", len(item))
        for i in item:
            query = 'insert into gs.dbo.ref_currency(item_name , item_code) values (?,?)'
            args = (i['name'], i['code'])
            cnxn.execute(query, args)
            cnxn.commit()
        print(next_page)
        URL_n = 'https://ows.goszakup.gov.kz' + next_page
#-------------------------------------------------------------------------------------
def ref_contract_year_type_6(**context):
    token = ('Bearer' + ' ' + 'f309ecc15d574833c45713da702049e2')
    headers_1 = { 'Content-Type' : 'application/json','Authorization':'none' }
    headers_1['Authorization'] = token
    next_page = 'none'
    URL_n = 'https://ows.goszakup.gov.kz/v3/refs/ref_contract_year_type'

    cnxn.execute('TRUNCATE TABLE gs.dbo.ref_contract_year_type')
    
    while next_page != '':
        r_n = requests.get(URL_n, headers=headers_1, verify=False).json()
        print(r_n)
        next_page = r_n['next_page']
        item = r_n['items']
        for i in item:
            query = 'insert into gs.dbo.ref_contract_year_type( item_id, item_name_ru , item_name_kz) values (?,?,?)'
            args = (i['id'], i['name_ru'], i['name_kz'])
            cnxn.execute(query, args)
            cnxn.commit()
        URL_n = 'https://ows.goszakup.gov.kz' + next_page
#------------------------------------------------------------------------------------
def ref_contract_agr_form_7(**context):
    token = ('Bearer' + ' ' + 'f309ecc15d574833c45713da702049e2')
    headers_1 = { 'Content-Type' : 'application/json','Authorization':'none' }
    headers_1['Authorization'] = token
    next_page = 'none'
    URL_n = 'https://ows.goszakup.gov.kz/v3/refs/ref_contract_agr_form'
    
    cnxn.execute('TRUNCATE TABLE gs.dbo.ref_contract_agr_form')
    
    while next_page != '':
        r_n = requests.get(URL_n, headers=headers_1, verify=False).json()
        next_page = r_n['next_page']
        item = r_n['items']
        for i in item:
            query = 'insert into gs.dbo.ref_contract_agr_form( item_id, item_name_ru , item_name_kz) values (?,?,?)'
            args = (i['id'], i['name_ru'], i['name_kz'])
            cnxn.execute(query, args)
            cnxn.commit()
        URL_n = 'https://ows.goszakup.gov.kz' + next_page
#-----------------------------------------------------------------------------------
def ref_contract_status_8(**context):
    token = ('Bearer' + ' ' + 'f309ecc15d574833c45713da702049e2')
    headers_1 = { 'Content-Type' : 'application/json','Authorization':'none' }
    headers_1['Authorization'] = token
    next_page = 'none'
    URL_n = 'https://ows.goszakup.gov.kz/v3/refs/ref_contract_status'
    
    cnxn.execute('TRUNCATE TABLE gs.dbo.ref_contract_status')
    
    while next_page != '':
        r_n = requests.get(URL_n, headers=headers_1, verify=False).json()
        next_page = r_n['next_page']
        item = r_n['items']
        for i in item:
            query = 'insert into gs.dbo.ref_contract_status( item_id, item_code, item_name_ru , item_name_kz) values (?, ?,?,?)'
            args = (i['id'],i['code'] ,i['name_ru'], i['name_kz'])
            cnxn.execute(query, args)
            cnxn.commit()
        URL_n = 'https://ows.goszakup.gov.kz' + next_page    
#----------------------------------------------------------------------------------
def ref_comm_roles_9(**context):
    token = ('Bearer' + ' ' + 'f309ecc15d574833c45713da702049e2')
    headers_1 = { 'Content-Type' : 'application/json','Authorization':'none' }
    headers_1['Authorization'] = token
    next_page = 'none'
    URL_n = 'https://ows.goszakup.gov.kz/v3/refs/ref_comm_roles'
    
    cnxn.execute('TRUNCATE TABLE gs.dbo.ref_comm_roles')
    
    while next_page != '':
        r_n = requests.get(URL_n, headers=headers_1, verify=False).json()
        print(r_n)
        next_page = r_n['next_page']
        item = r_n['items']
        for i in item:
            query = 'insert into gs.dbo.ref_comm_roles( item_id, item_code, item_name_ru , item_name_kz) values (?, ?,?,?)'
            args = (i['id'],i['code'] ,i['name_ru'], i['name_kz'])
            cnxn.execute(query, args)
            cnxn.commit()
        URL_n = 'https://ows.goszakup.gov.kz' + next_page
#-------------------------------------------------------------------------------
def ref_po_st_10(**context):
    token = ('Bearer' + ' ' + 'f309ecc15d574833c45713da702049e2')
    headers_1 = { 'Content-Type' : 'application/json','Authorization':'none' }
    headers_1['Authorization'] = token
    next_page = 'none'
    URL_n = 'https://ows.goszakup.gov.kz/v3/refs/ref_po_st'
    
    cnxn.execute('TRUNCATE gs.dbo.TABLE ref_po_st')
    
    while next_page != '':
        r_n = requests.get(URL_n, headers=headers_1, verify=False).json()
        print(r_n)
        next_page = r_n['next_page']
        item = r_n['items']
        for i in item:
            query = 'insert into gs.dbo.ref_po_st( item_id, item_code, item_name_ru , item_name_kz) values (?, ?,?,?)'
            args = (i['id'],i['code'] ,i['name_ru'], i['name_kz'])
            cnxn.execute(query, args)
            cnxn.commit()
        URL_n = 'https://ows.goszakup.gov.kz' + next_page
#-----------------------------------------------------------------------------
def ref_buy_status_11(**context):
    token = ('Bearer' + ' ' + 'f309ecc15d574833c45713da702049e2')
    headers_1 = { 'Content-Type' : 'application/json','Authorization':'none' }
    headers_1['Authorization'] = token
    next_page = 'none'
    URL_n = 'https://ows.goszakup.gov.kz/v3/refs/ref_buy_status'
    
    cnxn.execute('TRUNCATE TABLE gs.dbo.ref_buy_status')

    while next_page != '':
        r_n = requests.get(URL_n, headers=headers_1, verify=False).json()
        print(r_n)
        next_page = r_n['next_page']
        item = r_n['items']
        for i in item:
            query = 'insert into gs.dbo.ref_buy_status( item_id, item_code, item_name_ru , item_name_kz) values (?, ?,?,?)'
            args = (i['id'],i['code'] ,i['name_ru'], i['name_kz'])
            cnxn.execute(query, args)
            cnxn.commit()
        URL_n = 'https://ows.goszakup.gov.kz' + next_page
#---------------------------------------------------------------------------
def ref_type_trade_12(**context):
    token = ('Bearer' + ' ' + 'f309ecc15d574833c45713da702049e2')
    headers_1 = { 'Content-Type' : 'application/json','Authorization':'none' }
    headers_1['Authorization'] = token
    next_page = 'none'
    URL_n = 'https://ows.goszakup.gov.kz/v3/refs/ref_type_trade'
    
    cnxn.execute('TRUNCATE TABLE gs.dbo.ref_type_trade')
    
    while next_page != '':
        r_n = requests.get(URL_n, headers=headers_1, verify=False).json()
        print(r_n)
        next_page = r_n['next_page']
        item = r_n['items']
        for i in item:
            query = 'insert into gs.dbo.ref_type_trade( item_id, item_name_ru , item_name_kz) values (?,?,?)'
            args = (i['id'],i['name_ru'], i['name_kz'])
            cnxn.execute(query, args)
            cnxn.commit()
        URL_n = 'https://ows.goszakup.gov.kz' + next_page
#-------------------------------------------------------------------------
def ref_budget_type_13(**context):
    token = ('Bearer' + ' ' + 'f309ecc15d574833c45713da702049e2')
    headers_1 = { 'Content-Type' : 'application/json','Authorization':'none' }
    headers_1['Authorization'] = token
    next_page = 'none'
    URL_n = 'https://ows.goszakup.gov.kz/v3/refs/ref_budget_type'
    
    cnxn.execute('TRUNCATE TABLE gs.dbo.ref_budget_type')

    while next_page != '':
        r_n = requests.get(URL_n, headers=headers_1, verify=False).json()
        print(r_n)
        next_page = r_n['next_page']
        item = r_n['items']
        for i in item:
            query = 'insert into gs.dbo.ref_budget_type( item_id, item_code, item_name_ru , item_name_kz) values (?, ?,?,?)'
            args = (i['id'],i['code'] ,i['name_ru'], i['name_kz'])
            cnxn.execute(query, args)
            cnxn.commit()
        URL_n = 'https://ows.goszakup.gov.kz' + next_page
#-----------------------------------------------------------------------
def ref_amendm_agreem_justf_14(**context):
    token = ('Bearer' + ' ' + 'f309ecc15d574833c45713da702049e2')
    headers_1 = { 'Content-Type' : 'application/json','Authorization':'none' }
    headers_1['Authorization'] = token
    next_page = 'none'
    URL_n = 'https://ows.goszakup.gov.kz/v3/refs/ref_amendm_agreem_justif'
    
    cnxn.execute('TRUNCATE TABLE gs.dbo.ref_amendm_agreem_justf')

    while next_page != '':
        r_n = requests.get(URL_n, headers=headers_1, verify=False).json()
        print(r_n)
        next_page = r_n['next_page']
        item = r_n['items']
        for i in item:
            query = 'insert into gs.dbo.ref_amendm_agreem_justif( item_id, item_cname_ru, item_cname_kz, item_name_ru , item_name_kz) values (?, ?,?,?,?)'
            args = (i['id'],i['cname_ru'], i['cname_kz'], i['name_ru'], i['name_kz'])
            cnxn.execute(query, args)
            cnxn.commit()
        URL_n = 'https://ows.goszakup.gov.kz' + next_page
#----------------------------------------------------------------------
def ref_amendm_agreem_type_15(**context):
    token = ('Bearer' + ' ' + 'f309ecc15d574833c45713da702049e2')
    headers_1 = { 'Content-Type' : 'application/json','Authorization':'none' }
    headers_1['Authorization'] = token
    next_page = 'none'
    URL_n = 'https://ows.goszakup.gov.kz/v3/refs/ref_amendment_agreem_type'
    
    cnxn.execute('TRUNCATE TABLE gs.dbo.ref_amendm_agreem_type')

    while next_page != '':
        r_n = requests.get(URL_n, headers=headers_1, verify=False).json()
        print(r_n)
        next_page = r_n['next_page']
        item = r_n['items']
        for i in item:
            query = 'insert into gs.dbo.ref_amendment_agreem_type( item_id, item_name_ru , item_name_kz) values (?,?,?)'
            args = (i['id'], i['name_ru'], i['name_kz'])
            cnxn.execute(query, args)
            cnxn.commit()
        URL_n = 'https://ows.goszakup.gov.kz' + next_page  
    
    
#Tasks(15):    

task_1 = PythonOperator(task_id='ref_buy_lot_reject_reason1', provide_context=True, python_callable= ref_buy_lot_reject_reason_1, dag=dag)

task_2 = PythonOperator(task_id='ref_reason_2', provide_context=True, python_callable= ref_reason_2, dag=dag)

task_3 = PythonOperator(task_id='ref_contract_type_3', provide_context=True, python_callable= ref_contract_type_3, dag=dag)

task_4 = PythonOperator(task_id='ref_contract_cancel_4', provide_context=True, python_callable= ref_contract_cancel_4, dag=dag)

task_5 = PythonOperator(task_id='ref_currency_5', provide_context=True, python_callable= ref_currency_5, dag=dag)

task_6 = PythonOperator(task_id='ref_contract_year_type_6', provide_context=True, python_callable=ref_contract_year_type_6 , dag=dag)

task_7 = PythonOperator(task_id='ref_contract_agr_form_7', provide_context=True, python_callable= ref_contract_agr_form_7, dag=dag)

task_8 = PythonOperator(task_id='ref_contract_status_8', provide_context=True, python_callable = ref_contract_status_8, dag=dag)

task_9 = PythonOperator(task_id='ref_comm_roles_9', provide_context=True, python_callable = ref_comm_roles_9, dag=dag)

task_10 = PythonOperator(task_id='ref_po_st_10', provide_context=True, python_callable=ref_po_st_10, dag=dag)

task_11 = PythonOperator(task_id='ref_buy_status_11', provide_context=True, python_callable=ref_buy_status_11, dag=dag)

task_12 = PythonOperator(task_id='ref_type_trade_12', provide_context=True, python_callable=ref_type_trade_12, dag=dag) 

task_13 = PythonOperator(task_id='ref_budget_type_13', provide_context=True, python_callable=ref_budget_type_13, dag=dag)

task_14 = PythonOperator(task_id='ref_amendm_agreem_justf_14', provide_context=True, python_callable=ref_amendm_agreem_justf_14, dag=dag)

task_15 = PythonOperator(task_id='ref_amendm_agreem_type_15', provide_context=True, python_callable=ref_amendm_agreem_type_15, dag=dag)

#print('Zharaisyn')















