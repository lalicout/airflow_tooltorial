import pandas as pd
import sqlite3
from airflow.utils.edgemodifier import Label
from datetime import datetime, timedelta
from textwrap import dedent
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.models import Variable

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
def task1():
    
    sql_database_path = '/mnt/c/Users/laism/repositorios_indicium/airflow_tooltorial/data/Northwind_small.sqlite'
    table_name = '"Order"'
    csv_path = '/mnt/c/Users/laism/repositorios_indicium/airflow_tooltorial/target/output_orders.csv'
    
    conn = sqlite3.connect(sql_database_path)

    query = f'select * from {table_name}'
    df = pd.read_sql(query,conn)

    df.to_csv(csv_path, index= False)
    
    conn.close()


def task2():
    sql_database_path = '/mnt/c/Users/laism/repositorios_indicium/airflow_tooltorial/data/Northwind_small.sqlite'
    csv_path = '/mnt/c/Users/laism/repositorios_indicium/airflow_tooltorial/target/output_orders.csv'
    count_txt_path = '/mnt/c/Users/laism/repositorios_indicium/airflow_tooltorial/count.txt'
    
    df_csv = pd.read_csv(csv_path)
    
    conn = sqlite3.connect(sql_database_path)
    
    query = """select quantity , OrderId
    from OrderDetail"""
    df_sqlite = pd.read_sql(query, conn)
    
    conn.close()
    
    merged_df = pd.merge(df_csv, df_sqlite, left_on='Id', right_on='OrderId', how='inner')
    
    count_txt = str(merged_df[merged_df['ShipCity'] == 'Rio de Janeiro']['Quantity'].sum())

    with open(count_txt_path, 'w') as f:
        f.write(count_txt)
    

## Do not change the code below this line ---------------------!!#
def export_final_answer():
    import base64

    # Import count
    with open('count.txt') as f:
        count = f.readlines()[0]

    my_email = Variable.get("my_email")
    message = my_email+count
    message_bytes = message.encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    base64_message = base64_bytes.decode('ascii')

    with open("final_output.txt","w") as f:
        f.write(base64_message)
    return None
## Do not change the code above this line-----------------------##

with DAG(
    'DesafioAirflow',
    default_args=default_args,
    description='Desafio de Airflow da Indicium',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    dag.doc_md = """
        Esse é o desafio de Airflow da Indicium.
    """
    # Task definitions
    task1 = PythonOperator(
        task_id='task1',
        python_callable=task1,
    )
    
    task2 = PythonOperator(
        task_id='task2',
        python_callable=task2,
    )

    export_final_output = PythonOperator(
        task_id='export_final_output',
        python_callable=export_final_answer,
        provide_context=True
    )

    # Task dependencies
    task1 >> task2 >> export_final_output