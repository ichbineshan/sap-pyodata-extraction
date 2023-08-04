import pyodata
import requests

def make_connection():
    SERVICE_URL = 'https://sapgateway.kr.om:8001/sap/opu/odata/sap/ZLOLIPOP_v2_SRV/'
    user = 'KRD.DATALAKE'
    password = 'Sohar#2023'
    session = requests.Session()
    session.auth = (user, password)
    client = pyodata.Client(SERVICE_URL, session)
    return client

def fetch_data(last_fetched,records_till,batch_size):
    for i in range(last_fetched, records_till, batch_size):
        if i + batch_size >= records_till:
            chunk = sap_client.entity_sets.zpoctestSet.get_entities().top(records_till - i).skip(i).execute()
            current_status = records_till
        else :
            chunk = sap_client.entity_sets.zpoctestSet.get_entities().top(batch_size).skip(i).execute()
            current_status = i+batch_size
        for record in chunk:
            with open('data.csv', 'a') as f:
                f.write(f"{record.Id},{record.Name},{record.Lastname},{record.Address},{record.City},{record.Postalcode},{record.Country},{record.Email},{record.Phonenumber},{record.Birthdate},{record.Gender},{record.Nationality},{record.Preferredlanguage},{record.Creditlimit},{record.Registrationdate},{record.Lastpurchasedate},{record.Subscriptionstatus},{record.Loyaltypoints},{record.Customersegment},{record.Customercategory}\n")
        
        print("No. of records fetched till now :",current_status)

def total_records():
    num = sap_client.entity_sets.zpoctestSet.get_entities().count().execute()
    return num

sap_client = make_connection()
fetch_data(1900,2400,100)
print("THE TOTAL NUMBER OF RECORDS ARE : ", total_records())
