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
  database='etmam_dw_db' #DB Name
)
cursor = db.cursor()


def save_values_entity(application_id ,**kwargs):
            today = date.today()- timedelta(days=1)
            print("____________today____________")
            print(today)
            get_data_after_clean = application_id.split("_")
            ti = kwargs['ti']
            VIEW_ID = ti.xcom_pull(task_ids="DimAppData_"+str(today))
            data = ti.xcom_pull(task_ids='get_dim_app_'+application_id)
            application_id = get_data_after_clean[0]
            print("_____application_id_____");
            print(application_id);
            for data_views in VIEW_ID:
             if(data_views['nid'] == application_id):
                   print("_____Check_XCOM_data_views_____");
                   print(data_views);
                   application_id_full = data_views['application_id']
                   nid = data_views['nid']
                   service_type = 'test_airflow'
                   company_name = data_views['company_name']
                   project_name = data_views['project_name']
                   area_m2 = data_views['area_m2']
                   project_type = data_views['project_type']
                   region = data_views['region']
                   city = data_views['city']
                   branch = data_views['branch']
                   user_id = data_views['user_id']
                   create_date = data_views['create_date']
                   state = data_views['state']
                   days = data_views['days']
                   current_comment = data_views['current_comment']
                   is_overdue_incomplete = data_views['is_overdue_incomplete']

                   #insert Data
                   pg_insert = "INSERT INTO dim_applications (application_id,application_number, service_name,company_name,project_title,land_area_m2,project_type,region,city,branch,developer_id,post_date,duration_days,approve_reject_flag,is_overdue_incomplete,current_comment)  VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                   val = (application_id_full, nid,service_type, company_name, project_name,area_m2,project_type,region,city,branch,user_id,create_date,days,state,is_overdue_incomplete,current_comment)
                   cursor.execute(pg_insert, val)
                   db.commit()





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
    start_date=datetime(2022, 8, 16),
    schedule_interval="@daily",
    tags=['Dim applications'],
) as dag:
    application_id = []
    options_All_data = []
    today = str(date.today() - timedelta(days=1))
    count=0

    for dim_applications in get_Api_data_ids():
            options_All_data.append(dim_applications)
            application_id.append(dim_applications['nid']+"_"+today+"_"+str(count))
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
