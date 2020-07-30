import requests
import pyodbc
import datetime
from datetime import timedelta


def Token():
    URL_1 = 'https://www.ets-tender.kz/integration/json/User.Login/?login=galym.b@geometry.kz&password=Geometry987'
    headers_1 = {'Content-Type': 'application/x-www-form-urlencoded'}
    r = requests.post(URL_1, headers=headers_1)
    rr = r.json()
    token = rr['access_token']
    tokens = ('Bearer' + ' ' + rr['access_token'])
    print("First funct works")
    return token,tokens


def ID(token, tokens):
    now = int(datetime.datetime.now().timestamp())
    d_from = datetime.datetime(2020, 7, 2)
    to = 0
    sett = []
    while  to<=now:
        d_to = d_from + timedelta(days=7)
        to= int(d_to.timestamp())
        from_=int(d_from.timestamp())
        URL_2 = 'https://www.ets-tender.kz/integration/json/TradeProcedures.GetList?access_token=' + str(token) + '&date_from=' + str(from_) + '&date_to=' + str(to)
        d_from = d_from + timedelta(days=7)
        headers_2 = {'Authorization': 'Bearer  $ACCESS_TOKEN'}
        headers_2['Authorization'] = tokens
        r_2 = requests.get(URL_2, headers=headers_2).json()
        r_2 = r_2['trade_list']
        for i in r_2:
            ID = i['id']
            sett.append(ID)
    print("Number of elem in set:", len(sett))
    print("Second funct works")
    return sett

def FreshData(token, tokens):
    set_2=[]
    today = datetime.datetime.now()
    tom = today - timedelta(days=1)
    from_ = int(today.timestamp())
    to = int(tom.timestamp())
    URL_2 = 'https://www.ets-tender.kz/integration/json/TradeProcedures.GetList?access_token=' + str(token) + '&date_from=' + str(from_) + '&date_to=' + str(to)
    d_from = d_from + timedelta(days=7)
    headers_2 = {'Authorization': 'Bearer  $ACCESS_TOKEN'}
    headers_2['Authorization'] = tokens
    r_2 = requests.get(URL_2, headers=headers_2).json()
    r_2 = r_2['trade_list']
    for i in r_2:
        ID = i['id']
        sett_2.append(ID)
    print("Number of elem in set:", len(sett_2))
    print("Second funct works")
    return sett



def Connector(sett, tokens):
    conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                          'Server=LAPTOP-QHTHN2FI\MSSQLSERVER01;' 'Database=master;''Trusted_Connection=yes;',
                          autocommit=True)
    for j in sett:
        URL_3 = 'https://www.ets-tender.kz/integration/json/TradeProcedures.GetShortTrade?access_token=' + str(token) + '&id=' + str(j)
        headers_2 = {'Authorization': 'Bearer  $ACCESS_TOKEN'}
        print('after head 2')
        headers_2['Authorization'] = tokens
        r_3 = requests.get(URL_3, headers=headers_2).json()
        data = r_3['short_trade_procedure']
        print('Data', data)
        query = 'insert into Tender_2(id, number, _description, _type, procedure_place, trade_type, federal_law, organizer_name, organizer_iin, organizer_kpp, organizer_country, customer_name, customer_iin, customer_kpp, customer_country, _status, date_begin, date_end, date_trade_end, payment_terms, currency, alternate_offer, _url, lots_id, lots_description, lots_delivery_place, lots_quantity, lots_unit_name, lots_price, lots_price_no_tax, positions,os_number, os_name, publish_date, change_date, comment, delivery_address) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
        print('norm')
        args = (str(data['id']), data['number'], data['description'], str(data['type']), data['procedure_place'], data['trade_type'], data['federal_law'], data['organizer']['name'], str(data['organizer']['inn']), data['organizer']['kpp'], str(data['organizer']['country']), data['customer']['name'], str(data['customer']['inn']), data['customer']['kpp'], str(data['customer']['country']), data['status'], str(data['date_begin']),  str(data['date_end']), str(data['date_trade_end']), data['payment_terms'], data['currency'], str(data['alternate_offer']), data['url'], str(data['lots'][0]['id']), data['lots'][0]['name'], data['lots'][0]['description'], data['lots'][0]['delivery_place'][0], str(data['lots'][0]['quantity']), data['lots'][0]['unit_name'], str(data['lots'][0]['price']), str(data['lots'][0]['price_no_tax']), data['os_number'], data['os_name'], str(data['publish_date']), str(data['change_date']), data['comment'], str(data['delivery_address']))
        conn.execute(query, args)


token, tokens = Token() #done
sett = ID(token, tokens) #done
Connector(sett, tokens)
FreshData(token, tokens)
print("the end programm")