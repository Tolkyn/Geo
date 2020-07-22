import requests
import pyodbc

#-----------------------------------------------------------
def FIRST_STEP():
    URL_1='https://invictusfitness.perfectgym.com/Api/oauth/authorize'
    headers_1 = {'Content-Type': 'application/x-www-form-urlencoded'}
    payload = {'username':'apiuser', 'password':"h7lR.'M8xA", 'grant_type':'password'}
    r = requests.post(URL_1, headers=headers_1,data = payload )
    rr=r.json()
    global tokens
    tokens=('Bearer' +  ' ' + rr['access_token'])
#-----------------------------------------------------------------------------------------------
def SECOND_STEP(tokens):
    URL_2='https://invictusfitness.perfectgym.com/Api/Classes/Classes/1'
    headers_2 = {'Authorization':'NOne'}
    headers_2['Authorization']= tokens
    r_2 = requests.post(URL_2, headers=headers_2)


#-----------------------------------------------------------------------------------------------
a=0
def Request_visits(tokens,a):
    sett = []

    # Making request --------------------
    URL_3 = 'http://invictusfitness.perfectgym.com/Api/Users/ClubVisits/All?timestamp=0'
    headers_3 = {'Authorization': 'Bearer  $ACCESS_TOKEN'}
    headers_3['Authorization'] = tokens
    r_3 = requests.get(URL_3, headers=headers_3)
    r_3 = r_3.json()
    ele = r_3['elements']
    # Inserting data -----------
    for i in ele:
        sett.append(i['timestamp'])
        args = (i['enterDate'], i['exitDate'], i['club']['name'], i['club']['shortName'], i['club']['symbol'],
                i['club']['number'], i['club']['email'], i['club']['phoneNumber'], i['club']['latitude'],
                i['club']['longitude'], i['club']['timeZone'], i['club']['openDate'], i['club']['address']['line1'],
                i['club']['address']['line2'], i['club']['address']['city'], i['club']['address']['postalCode'],
                i['club']['address']['country'], i['club']['address']['countrySymbol'],
                i['club']['address']['stateSymbol'], i['club']['type'], i['club']['isHidden'],
                i['club']['clubPhotoUrl'], i['club']['id'], i['club']['timestamp'], i['club']['isDeleted'], i['userId'],
                i['id'], i['timestamp'], i['isDeleted'])
    a = sett[-1]
    print(args)
    #------------------------------------
'''    while len(sett)!=0:
        URL_3='http://invictusfitness.perfectgym.com/Api/Users/ClubVisits/All?timestamp='+str(a)
        headers_3={'Authorization': 'Bearer  $ACCESS_TOKEN'}
        headers_3['Authorization'] = tokens
        r_3=requests.get(URL_3, headers = headers_3)
        r_3=r_3.json()
        ele=r_3['elements']
        sett.clear()
        #Inserting data -----------
        for i in ele:
            sett.append(i['timestamp'])
            args=(i['enterDate'], i['exitDate'], i['club']['name'],i['club']['shortName'],i['club']['symbol'],i['club']['number'], i['club']['email'], i['club']['phoneNumber'], i['club']['latitude'], i['club']['longitude'], i['club']['timeZone'], i['club']['openDate'],i['club']['address']['line1'], i['club']['address']['line2'],i['club']['address']['city'],i['club']['address']['postalCode'],i['club']['address']['country'], i['club']['address']['countrySymbol'],i['club']['address']['stateSymbol'], i['club']['type'],i['club']['isHidden'], i['club']['clubPhotoUrl'], i['club']['id'],i['club']['timestamp'],i['club']['isDeleted'],i['userId'],i['id'],i['timestamp'],i['isDeleted'])
            a=sett[-1]
        print(a)
    else:
        print('End of iteration')'''
# while len(sett) != 0:
FIRST_STEP()
SECOND_STEP(tokens)
Request_visits(tokens,a)
print('The end')




