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
url = 'https://etmam-services.housing.gov.sa/ar/user/dim-state?date='+str(today)
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
            VIEW_ID = ti.xcom_pull(task_ids="DimState_"+str(today))
            data = ti.xcom_pull(task_ids='get_state_'+application_id)
            application_id = get_data_after_clean[0]
            print("_____application_id_____");
            print(application_id);
            for dim_state in VIEW_ID:
             if(dim_state['ID'] == application_id):
                nid = dim_state['ID']
                Application_Id = dim_state['Application_Id']
                Age = dim_state['Age']
                Stage = dim_state['Stage']
                old_State = dim_state['old_State']
                sate_machine_name = dim_state['sate_machine_name']
                Satge_complation_date = dim_state['Satge_complation_date']
                Comment = dim_state['Comment']
                responsible_employee = dim_state['responsible_employee']
                employee_username = dim_state['employee_username']
                pg_insert = "INSERT INTO dim_state (nid,Application_Id,Age,Stage,old_State,sate_machine_name,Satge_complation_date,Comment,responsible_employee,employee_username)  VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                val = (nid,Application_Id,Age,Stage,old_State,"sate_machine_name",Satge_complation_date,Comment,responsible_employee,employee_username)
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
    dag_id='dim_state',
    default_args=args,
    start_date=datetime(2022, 9, 28),
    schedule_interval="@daily",
    tags=['Dim State'],
) as dag:
    application_id = []
    options_All_data = []
    today = str(date.today() - timedelta(days=1))
    count=0

    for fact_app in get_Api_data_ids():
            options_All_data.append(fact_app)
            application_id.append(fact_app['ID']+"_"+today+"_"+str(count))
            count+=1


    start = DummyOperator(#start Dag
        task_id='start',
    )

    end = DummyOperator(
        task_id='end')

    DimState = PythonOperator(
            task_id='DimState_'+today,
            python_callable=all_fact_app_data,
            provide_context=True,
            op_kwargs={'options_All_data': options_All_data },

        )

    for application_id_data in application_id:

        get_state = PythonOperator(
            task_id='get_state_'+application_id_data,
            python_callable=get_values_entity,
            op_kwargs={'application_id': application_id_data },
            dag=dag,
        )

        ##task tree (perfom) and dependacy ->branches
        save_state = PythonOperator(
                task_id='save_state_'+application_id_data,
                python_callable=save_values_entity,
                op_kwargs={'application_id': application_id_data },
                dag=dag,
        )


        ##task tree (perfom) and dependacy
        start >> DimState >> save_state >> save_fact_app >> end
