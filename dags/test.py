from airflow import DAG
 
from datetime import datetime, timedelta
 
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
    
    
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.utcnow(),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
 
dag = DAG('xcom-test', default_args=default_args, schedule_interval=timedelta(days=1))
 
############################################
### xcom_push 함수로 xcom에 데이터 입력
############################################
def xcomPush(**context):   
    context['task_instance'].xcom_push(key='push_val_key', value=10)
 
 
 
############################################
### Return 시 자동으로 xcom에 데이터 입력
############################################
def xcomGen(**context):   
    return 20
 
 
############################################
### xcom_pull 함수로 xcom에 데이터 가져오기
############################################
def xcomPull(**context):
    model1_val = context['task_instance'].xcom_pull(task_ids='model1_task' ,key='push_val_key')
    model2_val = context['task_instance'].xcom_pull(task_ids='mode2_task')
    print ("*****************************************")
    print (f" model1_val : {model1_val}")
    print (f" model2_val : {model2_val}")
    print ("*****************************************")
    print (f" Max Value  : {max(model1_val,model2_val)}")
    print ("*****************************************")
     
     
 
 
 
startTask = BashOperator(task_id='startTask', bash_command='echo Start Task', dag=dag)
 
#### xcom 사용시 provide_context=True 옵션 추가 필요함
model1_task = PythonOperator(
        task_id = 'model1_task',
        python_callable = xcomPush,       
        provide_context=True,
        dag = dag
        )
 
mode2_task = PythonOperator(
        task_id = 'mode2_task',
        python_callable = xcomGen,       
        provide_context=True,
        dag = dag
        )
 
calMaxVal_task = PythonOperator(
        task_id = 'calMaxVal_task',
        python_callable = xcomPull,
        provide_context=True,
        dag = dag
        )
 
 
 
endTask = BashOperator(task_id='endTask', bash_command='echo End Task', dag=dag)
 
 
 
 
startTask >> [ model1_task , mode2_task ] >>  calMaxVal_task >> endTask
