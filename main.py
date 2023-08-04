# Importing the pyodata library for OData processing
import pyodata


# Importing the requests library for making HTTP requests
import requests


#Url and Credentials
SERVICE_URL = 'https://sapgateway.kr.om:8001/sap/opu/odata/sap/ZLOLIPOP_v2_SRV/'
user = 'KRD.DATALAKE'
password = 'Sohar#2023'


# Create a session object for making HTTP requests
session = requests.Session()


# Set the authentication credentials for the session
session.auth = (user, password)


# Create a pyodata client object with the specified service URL and session
client = pyodata.Client(SERVICE_URL, session)


# Get the schema information from the client
schema = client.schema


# Get the list of entity sets available in the schema
entitySets = client.schema.entity_sets


# Get the names of all entity sets available in the schema
entity_set_names = [es.name for es in entitySets]


# Retrieve entities from the "zpoctestSet" entity set and select specific properties (Id, Name, City)

numOfReq = 50

count = client.entity_sets.zpoctestSet.get_entities().count().execute()
print("THE TOTAL NUMBER OF RECORDS ARE : ", count)

batchSize = count//numOfReq + 1

for i in range(0,count,batchSize):
    chunk = client.entity_sets.zpoctestSet.get_entities().top(batchSize).skip(i).execute()
    for record in chunk:
        with open('data.csv','a') as f:
            # f.write(f"{record.Id},{record.Name},{record.City}\n")
            f.write(f"{record.Id},{record.Name},{record.Lastname},{record.Address},{record.City},{record.Postalcode},{record.Country},{record.Email},{record.Phonenumber},{record.Birthdate},{record.Gender},{record.Nationality},{record.Preferredlanguage},{record.Creditlimit},{record.Registrationdate},{record.Lastpurchasedate},{record.Subscriptionstatus},{record.Loyaltypoints},{record.Customersegment},{record.Customercategory}\n")


print(f"ALL {count} RECORDS FETCHED SUCCESSFULLY")            
