import base64
import six
import uuid
import io
import os
import requests
import psycopg2
import pprint
import locale
import time
from datetime import datetime,date, timedelta
import json
import urllib
import mysql.connector as mysql
from airflow import DAG
from datetime import datetime,date, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
default_args = {"owner": "Etmam"}

#from datetime import datetime, time ,timedelta
today = date.today()- timedelta(days=1)
url = 'https://etmam-services.housing.gov.sa/user/dim-applications?date='+str(today)
db = mysql.connect(
   host="localhost",
  user="root",
  password="Gtj#pC*QDwx[8rNt",
  port = 3306,
  database='etmam_tableau' #DB Name
)


cursor = db.cursor()


def save_values_entity(application_id ,**kwargs):
            today = date.today()- timedelta(days=1)
            print("____________today____________")
            print(today)
            get_data_after_clean = application_id.split("_")
            ti = kwargs['ti']
            VIEW_ID = ti.xcom_pull(task_ids="FactAppData_"+str(today))
            data = ti.xcom_pull(task_ids='get_fact_app_'+application_id)
            application_id = get_data_after_clean[0]
            print("_____application_id_____");
            print(application_id);
            for data_views in VIEW_ID:
             if(data_views['nid'] == application_id):
                    print("_____Check_XCOM_data_views_____");
                    print(data_views);
                    nid = fact_data['nid']
                    application_id = fact_data['project_id']
                    service_type = fact_data['type']
                    service_name = fact_data['type_name']
                    region = fact_data['Region']
                    city = fact_data['City']
                    create_date = fact_data['created_date']
                    state = fact_data['State']
                    dateLastState = fact_data['dateLastState']
                    pg_insert = "INSERT INTO Fact_applications (nid,application_id,service_id,service_name,region,city,created_date,current_state,final_complation_date)  VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    val = (nid ,application_id,service_type,"test",region,city,create_date,state,dateLastState)
                    cursor.execute(pg_insert, val)
                    db.commit()

def get_values_entity(application_id,**kwargs):
    ti = kwargs['ti']
    data = requests.get(url)
    return data.json()

def get_Api_data_ids():
    data = requests.get(url)
    return data.json()


def all_fact_app_data(options_All_data , **kwargs):
    print("All Fact App data:")
    print(options_All_data)
    return options_All_data

args = {
    'owner': 'Etmam',
    'provide_context': True
}

with DAG(
    dag_id='fact_app',
    default_args=args,
    start_date=datetime(2022, 9, 28),
    schedule_interval="@daily",
    tags=['Fact App'],
) as dag:
    application_id = []
    options_All_data = []
    today = str(date.today() - timedelta(days=1))
    count=0

    for fact_app in get_Api_data_ids():
            options_All_data.append(fact_app)
            application_id.append(fact_app['nid']+"_"+today+"_"+str(count))
            count+=1


    start = DummyOperator(#start Dag
        task_id='start',
    )

    end = DummyOperator(
        task_id='end')

    FactAppData = PythonOperator(
            task_id='FactAppData_'+today,
            python_callable=all_fact_app_data,
            provide_context=True,
            op_kwargs={'options_All_data': options_All_data },

        )

    for application_id_data in application_id:

        get_fact_app = PythonOperator(
            task_id='get_fact_app_'+application_id_data,
            python_callable=get_values_entity,
            op_kwargs={'application_id': application_id_data },
            dag=dag,
        )

        ##task tree (perfom) and dependacy ->branches
        save_fact_app = PythonOperator(
                task_id='save_fact_app_'+application_id_data,
                python_callable=save_values_entity,
                op_kwargs={'application_id': application_id_data },
                dag=dag,
        )


        ##task tree (perfom) and dependacy
        start >> FactAppData >> get_fact_app >> save_fact_app >> end
