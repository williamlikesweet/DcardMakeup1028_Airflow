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
import json
import pandas as pd
from myModels.random_header import Generate_Random_Header

def getprocess_0():
    pg_hook = PostgresHook(
        postgres_conn_id='postgres_1028makeup_localhost',
        schema='1028_makeup'
    )
    sql_stmt = """
        SELECT id FROM dcard_outline
        where process = 0;
    """
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    cursor.execute(sql_stmt)
    return cursor.fetchone()


def update_process1():
    post_ID = Crawl_ID_Content()
    post_ID =  str(post_ID)[1:-2]
    pg_hook = PostgresHook(
        postgres_conn_id='postgres_1028makeup_localhost',
        schema='1028_makeup'
    )
    update_process = f"""
        UPDATE dcard_outline SET process = 1 
        where id = '{post_ID}';
    """
    pg_hook.run(update_process)
    return

def checkDcard_id(postid):
    pg_hook = PostgresHook(
        postgres_conn_id='postgres_1028makeup_localhost',
        schema='1028_makeup'
    )
    parameter= postid
    sql_stmt = f"""
        SELECT postid FROM dcard_makeup_content             
        where postid = '{parameter}';
    """
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    cursor.execute(sql_stmt)
    return cursor.fetchone()

def Crawl_ID_Content():
    post_ID = getprocess_0()
    # getMinid()fetchone Value is [] ex.. [238425989,] ; fetchall Value is [tuple] ex.. [(238431085,)]
    link = 'https://www.dcard.tw/service/api/v2/posts/' + str(post_ID)[1:-2]
    requ = requests.get(link,headers=Generate_Random_Header())
    rejs = json.loads(requ.content.decode(encoding='utf8'))
    if requ.status_code != requests.codes.ok:
        pass
    else:
        id_data = pd.DataFrame(
        data=[{
        'ID':rejs['id'],
        'title':rejs['title'],
        'content':rejs['content'],
        'excerpt':rejs['excerpt'],
        'commentCount':rejs['commentCount'],
        'gender':rejs['gender'],
        'likeCount':rejs['likeCount'],
        'topics':rejs['topics'],
        'createdAt':rejs['createdAt']
        }], columns=['ID','title','content','excerpt','commentCount','gender','likeCount','topics','createdAt'])
        pg_hook = PostgresHook(
        postgres_conn_id='postgres_1028makeup_localhost',
        schema='1028_makeup'
        )
        for postid,title,content,excerpt,commentCount,gender,likeCount,topics,createdAt in zip(
            id_data['ID'].tolist(), id_data['title'].tolist(), id_data['content'].tolist(), 
            id_data['excerpt'].tolist(), id_data['commentCount'].tolist(), id_data['gender'].tolist(), 
            id_data['likeCount'].tolist(), id_data['topics'].tolist(), id_data['createdAt'].tolist()):
            if not checkDcard_id(postid):
                dts_insert = "insert into dcard_makeup_content (postid, title, content, excerpt, commentCount, gender, likeCount, topics, createdAt) values (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
                pg_hook.run(dts_insert, parameters=(postid, title, content, excerpt, commentCount, gender, likeCount, topics, createdAt))
            else:
                pass
    return post_ID

local_tz = pendulum.timezone("Asia/Shanghai")
default_args = {
    "owner": "KuanLauBan", 
    "retries": 3,
    'tzinfo': local_tz
}
with DAG(
    dag_id='1028_Dcard_Content',
    default_args = default_args,
    start_date= datetime(2022, 3, 26),
    schedule_interval='*/5 * * * *',
    catchup=False

) as dag:
    createMakeup_content = PostgresOperator(
        task_id='create_1028makeup_content_table',
        postgres_conn_id='postgres_1028makeup_localhost',
        sql="""
            create table if not exists dcard_makeup_content (
                postid INTEGER,
                title varchar(255),
                content text,
                excerpt varchar,
                commentCount INTEGER,
                gender varchar,
                likeCount INTEGER,
                topics varchar,
                createdAt date,
                primary key (postId)
            )
        """,
    )

    makeup_content_inser_DB= PythonOperator(
        task_id="makeup_Content_inser_DB",
        python_callable= Crawl_ID_Content,
        provide_context= True,
    )

    update_dcard_outline_process= PythonOperator(
        task_id="update_process1",
        python_callable= update_process1,
        provide_context= True,
    )

    createMakeup_content >> makeup_content_inser_DB >> update_dcard_outline_process

