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

def get_postgres_data(x):
    pg_hook = PostgresHook(
        postgres_conn_id='postgres_localhost',
        schema='test'
    )
    parameter= x
    sql_stmt = f"""
        SELECT (id) FROM prettygirl             
        where id = '{parameter}';
    """
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    cursor.execute(sql_stmt)
    return cursor.fetchone()

def peekme_paser():
    url = "https://www.peekme.cc/category/categoryPostList?catId=26&page=1"

    datadate =[]
    datahref =[]
    r = requests.get(url,headers=Generate_Random_Header())
    soup = BeautifulSoup(r.text)
    for x in soup.find_all('div','post-wrapper'):
        data1 = x.li.text.strip()
        datadate.append(data1)
        data2 = x.a['href']
        datahref.append(data2)
    # return str(tuple(datalist))[1:-1]
    return datadate,datahref

def sharefie_paser():
    url = "https://www.sharefie.net/load_content.php?pageno=1&cateidsel=2&q=&randpost=0"
    r = requests.get(url,headers=Generate_Random_Header())
    soup = BeautifulSoup(r.text,"html.parser")
    site_json=json.loads(soup.text)
    df = pd.DataFrame(site_json['postdata'])
    return df

def peekme_insert():
    pg_hook = PostgresHook(
        postgres_conn_id='postgres_localhost',
        schema='test'
    )
    datadate= peekme_paser()[0]
    datahref= peekme_paser()[1]
    idlst = [s.split('/')[2] for s in datahref]

    for x,y in zip(idlst,datadate):
        if not get_postgres_data(x):
            dts_insert = "insert into prettygirl (id, https, createdAt) values (%s, %s, %s)"
            pg_hook.run(dts_insert, parameters=(x, "https://www.peekme.cc/post/"+x, y))
        else:
            pass
        
    return

def sharefie_insert():
    pg_hook = PostgresHook(
        postgres_conn_id='postgres_localhost',
        schema='test'
    )
    df = sharefie_paser()

    for x,y in zip(df['pid'],df['datecreated']):
        if not get_postgres_data(x):
            dts_insert = "insert into prettygirl (id, https, createdAt) values (%s, %s, %s)"
            pg_hook.run(dts_insert, parameters=(x, "https://www.sharefie.net/post/"+x, y))
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
    dag_id='PrettyGirl_Paser',
    default_args = default_args,
    start_date= datetime(2022, 3, 25),
    schedule_interval='0 */4 * * *',
    catchup=False

) as dag:
    createTable = PostgresOperator(
        task_id='create_postgres_table',
        postgres_conn_id='postgres_localhost',
        sql="""
            create table if not exists prettygirl (
                id INTEGER,
                https character varying,
                createdAt date,
                primary key (id)
            )
        """,
    )

    peekme_insert_db = PythonOperator(
        task_id="peekme_insert_db",
        python_callable= peekme_insert,
        provide_context= True,
    )

    sharefie_insert_db = PythonOperator(
        task_id="sharefie_insert_db",
        python_callable= sharefie_insert,
        provide_context= True,
    )


    createTable >> [peekme_insert_db,sharefie_insert_db] 


    



#---------------------------TEST-------------------------------------------------------------
    # task2 = PostgresOperator(
    #     task_id='insert_into_table',
    #     postgres_conn_id='postgres_localhost',
    #     # params={'data': 'cccc', 'end_date': '2020-12-31'},
    #     # sql="""
    #     #     insert into dag_runs (dt, dag_id) values ('{{ params.end_date }}', '{{ params.data }}')
    #     # """

    #     params={'data': ETL_data()},
    #     sql="""
    #         insert into dag_runs (dt, dag_id) values {{ params.data }} ;
    #     """,
    # )

    # Delete
    # task3 = PostgresOperator(
    #     task_id='delete_data_from_table',
    #     postgres_conn_id='postgres_localhost',
    #     sql="""
    #         delete from dag_runs where dt = '{{ ds }}' and dag_id = '{{ dag.dag_id }}';
    #     """
    # )

    # for one data
    # def ETL_data_for_Postgres():
    #     newlist=[]
    #     for i in paser():
    #         if i in get_postgres_data():
    #             print(i)
    #         else:
    #             newlist.append(i)
    #     return str(tuple(newlist))[1:-1]

    # get_pos_data = PythonOperator(
    #     task_id="get_pos_data",
    #     python_callable= get_postgres_data,
    #     provide_context= True,
    #     do_xcom_push= True,
    # )

    # example
    # def write_to_pg(**kwargs):
    #     execution_time = kwargs['ts']
    #     run_time = dt.datetime.utcnow()
    #     print('Writing to pg', runtime, execution_time)
    #     dts_insert = 'insert into dts (runtime, execution_time) values (%s, %s)'
    #     pg_hook.run(dts_insert, parameters=(runtime, execution_time,))


#----------Call Task-------------------------------------------------------------
    # get_missing_data = MysqlToGCS(
    #     task_id = 'get_orders_from_aws',
    #     sql = create_sql_query(),
    #     ...

    # get_missing_data = MysqlToGCS(
    #     task_id = 'get_orders_from_aws',
    #     sql = f"select * from orders where id in ({get_missing_ids()})",
    #     ...