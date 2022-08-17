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
url = 'https://etmam-services.housing.gov.sa/user/dim-applications'

db = mysql.connect(
  host="localhost",
  user="root",
  password="Gtj#pC*QDwx[8rNt",
  port = 3306,
  database='etmam_dw_db' #DB Name
)

def save_values_entity(application_id ,**kwargs):
            today = date.today()- timedelta(days=1)
            get_data_after_clean = application_id.split("_")
            ti = kwargs['ti']
            VIEW_ID = ti.xcom_pull(task_ids="DimAppData_"+str(today))
            data = ti.xcom_pull(task_ids='get_dim_app_'+application_id)
            print("_____Check_XCOM_VIEW______");
            print(VIEW_ID);
            print("_____Check_XCOM_DATA_____");
            print(data);



def get_values_entity(application_id,**kwargs):
    ti = kwargs['ti']
    data = requests.get(url)
    return data.json()

def get_Api_data_ids():
    data = requests.get(url)
    return data.json()


def all_dim_app_data(options_All_data , **kwargs):
    print("All Dim App data:")
    print(options_All_data)
    return options_All_data

args = {
    'owner': 'Etmam',
    'provide_context': True
}

with DAG(
    dag_id='dim_applications',
    default_args=args,
    start_date=datetime(2022, 8, 14),
    schedule_interval="@daily",
    tags=['Dim applications'],
) as dag:
    application_id = []
    options_All_data = []
    today = str(date.today() - timedelta(days=1))
    count=0

    for dim_applications in get_Api_data_ids():
            options_All_data.append(dim_applications)
            application_id.append(dim_applications['application_id']+"_"+today+"_"+str(count))
            count+=1


    start = DummyOperator(#start Dag
        task_id='start',
    )

    end = DummyOperator(
        task_id='end')

    DimAppData = PythonOperator(
            task_id='DimAppData_'+today,
            python_callable=all_dim_app_data,
            provide_context=True,
            op_kwargs={'options_All_data': options_All_data },

        )

    for application_id_data in application_id:

        get_dim_app = PythonOperator(
            task_id='get_dim_app_'+application_id_data,
            python_callable=get_values_entity,
            op_kwargs={'application_id': application_id_data },
            dag=dag,
        )

        ##task tree (perfom) and dependacy ->branches
        save_dim_app = PythonOperator(
                task_id='save_dim_app_'+application_id_data,
                python_callable=save_values_entity,
                op_kwargs={'application_id': application_id_data },
                dag=dag,
        )


        ##task tree (perfom) and dependacy
        start >> DimAppData >> get_dim_app >> save_dim_app >> end
