from airflow import DAG  
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

from bs4 import BeautifulSoup
import requests
from datetime import datetime
import pendulum
import time
import json
import pandas as pd
from myModels.random_header import Generate_Random_Header


def Crawl(link):
    requ = requests.get(link,time.sleep(0.5),headers=Generate_Random_Header())
    rejs = json.loads(requ.content.decode(encoding='utf8'))
    return(rejs)

def NewCrawl():
    link = 'https://www.dcard.tw/service/api/v2/forums/makeup/posts?popular=false&limit=100'
    return(pd.DataFrame(Crawl(link)))

def checkDcard_id(x):
    pg_hook = PostgresHook(
        postgres_conn_id='postgres_1028makeup_localhost',
        schema='1028_makeup'
    )
    parameter=x
    sql_stmt = f"""
        SELECT id FROM dcard_outline             
        where id = '{parameter}';
    """
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    cursor.execute(sql_stmt)
    return cursor.fetchone()

def getMinid():
    pg_hook = PostgresHook(
        postgres_conn_id='postgres_1028makeup_localhost',
        schema='1028_makeup'
    )
    sql_stmt = """
        SELECT min(id) FROM dcard_outline ;
    """
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    cursor.execute(sql_stmt)
    return cursor.fetchone()


def dcardOutline_update():
    df_data = NewCrawl()
    pg_hook = PostgresHook(
        postgres_conn_id='postgres_1028makeup_localhost',
        schema='1028_makeup'
    )
    for x,y in zip(df_data['id'],df_data['createdAt']):
        if not checkDcard_id(x):
            dts_insert = "insert into dcard_outline (id, process, createdat) values (%s, %s, %s)"
            pg_hook.run(dts_insert, parameters=(x, 0, y))
        else:
            pass
    return

def dcardOutline_past():
    MinId = getMinid()
    # getMinid()fetchone Value is [] ex.. [238425989,] ; fetchall Value is [tuple] ex.. [(238425989,)]
    link = "https://www.dcard.tw/service/api/v2/forums/makeup/posts?popular=false&limit=100&before="+str(MinId)[1:-2]
    df_data = pd.DataFrame(Crawl(link))
    pg_hook = PostgresHook(
        postgres_conn_id='postgres_1028makeup_localhost',
        schema='1028_makeup'
    )
    for x,y in zip(df_data['id'],df_data['createdAt']):
        if not checkDcard_id(x):
            dts_insert = "insert into dcard_outline (id, process, createdat) values (%s, %s, %s)"
            pg_hook.run(dts_insert, parameters=(x, 0, y))
        else:
            pass
    return


local_tz = pendulum.timezone("Asia/Shanghai")
default_args = {
    "owner": "KuanLauBan", 
    "retries": 3,
    'tzinfo': local_tz
}
with DAG(
    dag_id='1028_Dcard_ID',
    default_args = default_args,
    start_date= datetime(2022, 3, 26),
    schedule_interval='*/60 * * * *',
    catchup=False

) as dag:
    createMakeup = PostgresOperator(
        task_id='create_1028makeup_table',
        postgres_conn_id='postgres_1028makeup_localhost',
        sql="""
            create table if not exists dcard_outline (
                id INTEGER,
                process INTEGER,
                createdAt date,
                primary key (id)
            )
        """,
    )

    makeup_insert_db_update = PythonOperator(
        task_id="dcardOutline_update",
        python_callable= dcardOutline_update,
        provide_context= True,
    )

    makeup_insert_db_past = PythonOperator(
        task_id="dcardOutline_past",
        python_callable= dcardOutline_past,
        provide_context= True,
    )

    createMakeup >> [makeup_insert_db_update,makeup_insert_db_past]

