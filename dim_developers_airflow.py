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


url = 'https://etmam-services.housing.gov.sa/user/dim-developers?date='+str(today)

db = mysql.connect(
   host="localhost",
  user="root",
  password="Gtj#pC*QDwx[8rNt",
  port = 3306,
  database='etmam_tableau' #DB Name
)

cursor = db.cursor()


def save_values_entity(user_id ,**kwargs):
            today = date.today()- timedelta(days=1)
            print("____________today____________")
            print(today)
            get_data_after_clean = user_id.split("_")
            ti = kwargs['ti']
            VIEW_ID = ti.xcom_pull(task_ids="DimDevelopersData_"+str(today))
            data = ti.xcom_pull(task_ids='get_dvelopers_app_'+user_id)
            user_id = get_data_after_clean[0]
            print("_____user_id_____");
            print(user_id);
            for data_views in VIEW_ID:
             if(data_views['uid'] == user_id):
                     print("_____Check_XCOM_data_views_____");
                     print(data_views);
                     email = data_views['email']
                     full_name = data_views['full_name']
                     national_id = data_views['national_id']
                     phone = data_views['phone']
                     role = data_views['role']
                     uid = data_views['uid']
                     user_created = data_views['user_created']
                     register_identity = data_views['register_identity']
                     short_establish = data_views['short_establish']
                     developer_type = data_views['developer_type']
                     cr_name = data_views['cr_name']
                     Beneficiry_cr = data_views['Beneficiry_cr']
                     #insert Data
                     sql = "INSERT INTO dim_developers (user_id, national_id, full_name,email,phone,role,user_created,register_identity,short_establishment,developer_type,cr_name,beneficiry_cr)  VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                     val = (uid, national_id, full_name, email,phone,role,user_created,register_identity,short_establish,developer_type,cr_name,Beneficiry_cr)
                     cursor.execute(sql, val)
                     db.commit()





def get_values_entity(application_id,**kwargs):
    ti = kwargs['ti']
    data = requests.get(url)
    return data.json()

def get_Api_data_ids():
    data = requests.get(url)
    return data.json()


def all_developers_data(options_All_data , **kwargs):
    print("All Developers data:")
    print(options_All_data)
    return options_All_data

args = {
    'owner': 'Etmam',
    'provide_context': True
}

with DAG(
    dag_id='dim_developers',
    default_args=args,
    start_date=datetime(2022, 9, 28),
    schedule_interval="@daily",
    tags=['Dim Developers'],
) as dag:
    user_id = []
    options_All_data = []
    today = str(date.today() - timedelta(days=1))
    count=0

    for dim_developers in get_Api_data_ids():
            options_All_data.append(dim_developers)
            user_id.append(dim_developers['uid']+"_"+today+"_"+str(count))
            count+=1


    start = DummyOperator(#start Dag
        task_id='start',
    )

    end = DummyOperator(
        task_id='end')

    DimDevelopersData = PythonOperator(
            task_id='DimDevelopersData_'+today,
            python_callable=all_developers_data,
            provide_context=True,
            op_kwargs={'options_All_data': options_All_data },

        )

    for user_id_data in user_id:

        get_developer_app = PythonOperator(
            task_id='get_dvelopers_app_'+user_id_data,
            python_callable=get_values_entity,
            op_kwargs={'user_id': user_id_data },
            dag=dag,
        )

        ##task tree (perfom) and dependacy ->branches
        save_developer_app = PythonOperator(
                task_id='save_developers_app_'+user_id_data,
                python_callable=save_values_entity,
                op_kwargs={'user_id': user_id_data },
                dag=dag,
        )


        ##task tree (perfom) and dependacy
        start >> DimDevelopersData >> get_developer_app >> save_developer_app >> end
