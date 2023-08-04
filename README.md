# sap-pyodata-extraction
Using the Python's pyodata library to extract data from the OData layer  

Install the library using -> pip install pyodata  

___FILES___  
main.py - level 1 implementation of sap data extraction using odata layer.  
functions.py - functions to make connection and fetch data using fetch functions.  
metadata.txt - to maintain the count of last records fetched.  
data.csv - storing the fetched data.  
checker.py - to check whether the data has been updated in the server side, if yes then fetch it and dump it with the already existing data, and update the metadata.  
 