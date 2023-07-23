from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2
import pandas as pd
import smtplib

def conexion_redshift():
    url="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
    data_base="data-engineer-database"
    user="fredafranco13_coderhouse"
    pwd= "uuS4769kWq"
    conn = psycopg2.connect(
        host=url,
        dbname=data_base,
        user=user,
        password=pwd,
        port='5439'
    )
    print("Conexión establecida a Redshift")
    return conn

def cargar_data():
    conn = conexion_redshift()
    ruta_archivos='/app/'
    product = pd.read_excel(ruta_archivos+'Datos bancos.xlsx')
    cargar_en_redshift(conn, 'Datos_entrega2', product)
    print("Datos cargados a Redshift")

def cargar_en_redshift(conn, table_name, dataframe):
    cursor = conn.cursor()
    dtypes = dataframe.dtypes
    cols = list(dtypes.index)
    tipos = list(dtypes.values)
    type_map = {'int64': 'INT','float64': 'FLOAT','object': 'VARCHAR(50)'}
    sql_dtypes = [type_map[str(dtype)] for dtype in tipos]
    column_defs = [f"{name} {data_type}" for name, data_type in zip(cols, sql_dtypes)]
    table_schema = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join(column_defs)}
        );
        """
    cursor.execute(table_schema)
    for i, row in dataframe.iterrows():
        insert_query = f"INSERT INTO {table_name} VALUES {tuple(row.values)}"
        cursor.execute(insert_query)
    conn.commit()
    cursor.close()

def enviar():
    try:
        x=smtplib.SMTP('smtp.gmail.com',587)
        x.starttls()
        x.login('fredafranco13@gmail.com','46399860')
        subject='El proceso ha sido ejecutado correctamente '
        body_text='El proceso de carga a Amazon Redshift ha sido enviado correctamente '
        message='Subject: {}\n\n{}'.format(subject,body_text)
        x.sendmail('lucianadelamerced@gmail.com','lucianadelamerced@gmail.com',message)
        print('Exito')
    except Exception as exception:
        print(exception)
        print('Failure')

default_args={
    'owner': 'DavidBU',
    'start_date': datetime(2022,9,6)
}

with DAG('my_dag', default_args=default_args, start_date=datetime(2023, 6, 16), schedule_interval='@weekly') as dag:
    task_31 = PythonOperator(
        task_id="conexion_BD",
        python_callable=conexion_redshift,
    )

    task_32 = PythonOperator(
        task_id='cargar_data',
        python_callable=cargar_data,
    )

    tarea_33=PythonOperator(
        task_id='dag_envio',
        python_callable=enviar
    )

    task_31 >> task_32 >> tarea_33
