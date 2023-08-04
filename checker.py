from functions import *
from time import sleep

def data_loading():
    try:
        with open('metadata.txt','r') as f:
            cnt = f.read()
        cnt = int(cnt)
    except Exception as e:
        cnt = 0
        print(e)
        print("Starting from first row...")
        with open('metadata.txt','w') as f:
            f.write(str(cnt))    
        
    sap_client = make_connection()
    total_records_in_db = total_records(sap_client)
    print(total_records_in_db)

    if (total_records_in_db > cnt):
        fetch_data(sap_client,
                last_fetched=cnt,
                records_till=total_records_in_db,
                batch_size=25000)

while True:
    data_loading()
    sleep(25)        