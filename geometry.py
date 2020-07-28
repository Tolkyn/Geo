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
    return token ,tokens


def Getting_ID(token, tokens):
    now = int(datetime.datetime.now().timestamp())
    to = 0
    if to<=now:
        for i in range(4):
            d_from =datetime.datetime(2020, 7, 2) + i*timedelta(days=7)
            d_to = d_from + timedelta(days=7)
            to = int(d_to.timestamp())
            print('Date from and to')
            print(d_from)
            from_=int(d_from.timestamp())
            print(d_to)
            global headers_2, sett
            URL_2 = 'https://www.ets-tender.kz/integration/json/TradeProcedures.GetList?access_token=' + str(token) + '&date_from=' + str(from_) + '&date_to=' + str(to)
            headers_2 = {'Authorization': 'Bearer  $ACCESS_TOKEN'}
            headers_2['Authorization'] = tokens
            r_2 = requests.get(URL_2, headers=headers_2).json()
            print(r_2)
            r_2 = r_2['trade_list']
            print(len(r_2))
            sett = []
            for i in r_2:
                ID = i['id']
                sett.append(ID)
                conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                                  'Server=LAPTOP-QHTHN2FI\MSSQLSERVER01;' 'Database=master;''Trusted_Connection=yes;',autocommit=True)
            for j in sett:
                URL_3 = 'https://www.ets-tender.kz/integration/json/TradeProcedures.GetShortTrade?access_token=' + str(
                        token) + '&id=' + str(j)
                r_3 = requests.get(URL_3, headers=headers_2).json()
                data = r_3['short_trade_procedure']
                print(data)
                query = 'insert into Tender(procedure_place , trade_type, federal_law, organizer_name, organizer_iin ,organizer_kpp,organizer_country) values(?,?,?,?,?,?,?)'
                args = (data['procedure_place'], data['trade_type'], data['federal_law'], data['organizer']['name'], data['organizer']['inn'], data['organizer']['kpp'], data['organizer']['country'])
                conn.execute(query, args)
    else:
        d_to = now  # we should write a func


token, tokens = Token()
Getting_ID(token, tokens)

print("the end programm")