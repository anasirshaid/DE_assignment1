from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import psycopg2
from faker import Faker
import config
import pandas as pd
from datetime import datetime
import csv
import json
####################################### Creat a Data Set to be used for assignments ################
def Create_dummy_Data():
    output = open('/opt/airflow/logs/data.csv','w')
    fake=Faker()
    header = ['name','age','street','city','state','zip','lng','lat']
    mywriter=csv.writer(output)
    mywriter.writerow(header)

    for r in range(1000):
        row = [fake.name(),fake.random_int(min=18,max=80,step=1),fake.street_address(),fake.city(),fake.state(),fake.zipcode(),fake.longitude(),fake.latitude()]
        print (row)
        mywriter.writerow(row)
    output.close()

###################################### Load the Data created Data set to postgres ##################
def postgres_dataload():
    connection = psycopg2.connect(user = "airflow",
    password = "airflow",
    host = "postgres",
    port = "5432",
    database = "postgres")
    cursor = connection.cursor()
   
    creat_table_query = '''CREATE TABLE IF NOT EXISTS datasetassign1
    (name TEXT NOT NULL,
    age TEXT NOT NULL,
    street TEXT NOT NULL,
    city TEXT NOT NULL,
    state TEXT NOT NULL,
    zip TEXT NOT NULL,
    lng TEXT NOT NULL,
    lat TEXT NOT NULL);'''

    cursor.execute(creat_table_query)
    with open('/opt/airflow/logs/data.csv','r') as fimport :
        #next(fimport)#skip the header row
        cursor.copy_from(fimport,'datasetassign1', sep=',') 

    connection.commit() 

############################### Export file from postgresql ###########################################
def export_csv_frompostgresql(): 
    connection = psycopg2.connect(user = "airflow",
    password = "airflow",
    host = "postgres",
    port = "5432",
    database = "postgres")
    cursor = connection.cursor()    

    sql = "COPY (SELECT * FROM datasetassign1) TO STDOUT WITH CSV DELIMITER ','"
    with open('/opt/airflow/logs/Exported_data.csv', 'w') as fexport:
        cursor.copy_expert(sql, fexport)
    
    connection.commit() 

######################################### Convert the CSV to JSON ####################################
def CSV_to_JSON():
    DF = pd.read_csv('/opt/airflow/logs/Exported_data.csv')
    DF.to_json('/opt/airflow/logs/converted_data.json',orient='records')

######################################### load Json to MongoDB ######################################
import pymongo
def JSONtoMONGODB ():
    mng_client = pymongo.MongoClient(host = "mongo",
                                    port= 27017,
                                    username = "root", 
                                    password = "DECourse")

    mng_db = mng_client['Assignment1DB']
    collection_name = 'Assignment1_collections'
    db_cm = mng_db[collection_name]

    with open('/opt/airflow/logs/converted_data.json') as fjson:
        file_data = json.load(fjson)
   
    db_cm.remove()
    db_cm.insert(file_data)
    mng_client.close()

######################################### Pipe-Line DAG's ###########################################
my_dag = DAG(
    dag_id = 'assignment1',
    #schedule_interval = "0 0 * * *",
    start_date=datetime(2021,5,5)
)

Create_Data = PythonOperator(task_id='Create_data',
                             python_callable=Create_dummy_Data,
                             dag=my_dag)

load_to_postgresqldb = PythonOperator(task_id='load_CSV_into_postgresql',
                             python_callable=postgres_dataload,
                             dag=my_dag)

export_csvdata_frompostgresql = PythonOperator(task_id='Export_csv_from_postgresql',
                             python_callable=export_csv_frompostgresql,
                             dag=my_dag)

Convert_CSV_TO_JSON = PythonOperator(task_id='CSVtoJSON',
                             python_callable=CSV_to_JSON,
                             dag=my_dag)

load_json_MongoDB = PythonOperator(task_id='load_JSON_to_MongoDB',
                             python_callable=JSONtoMONGODB,
                             dag=my_dag)                             

Create_Data >> load_to_postgresqldb >> export_csvdata_frompostgresql >> Convert_CSV_TO_JSON >> load_json_MongoDB
