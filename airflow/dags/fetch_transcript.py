
import os
import requests
import google.cloud.storage as gcs
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.models.baseoperator import chain
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, Date, insert, text
from google.cloud.sql.connector import Connector
import pg8000

owner = 'BigDataIA-Summer2023-Team2'
repo = 'MAEC-A-Multimodal-Aligned-Earnings-Conference-Call-Dataset-for-Financial-Risk-Prediction'
directory_path = '/MAEC_Dataset'
token = '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'
gcs_bucket_name = 'damg-github-dump'
gcs_project_id = 'assignment2-390320'


storage_client = gcs.Client(project=gcs_project_id)
# bucket = storage_client.get_bucket(gcs_bucket_name)
bucket = storage_client.bucket(gcs_bucket_name)




default_args = {
    'owner': 'Damg-team2',
    'start_date': datetime(2023, 6, 19),
}


dag = DAG(
    dag_id="github_to_gcs_to_postgres",
    # Run daily midnight to fetch metadata from github
    schedule="0 0 * * *",   # https://crontab.guru/
    start_date=days_ago(0),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    tags=["assignment2", "damg7245", "github_to_gcs_to_postgres"],
)
# # Create the DAG instance
# # with DAG('github_to_gcs', default_args=default_args, schedule_interval=None) as dag:
# def get_directory_contents(owner, repo, directory_path, token):
#         try:
#             bucket.reload()
#             print(f'Bucket {gcs_bucket_name} exists.')
#         except:
#             print(f'Bucket {gcs_bucket_name} does not exist.')
#             bucket.storage_class = "STANDARD"
#             new_bucket = storage_client.create_bucket(bucket, location="us-east1")
        
        
#         headers = {'Authorization': f'token {token}'}
#         api_url = f'https://api.github.com/repos/{owner}/{repo}/contents/{directory_path}'
#         response = requests.get(api_url, headers=headers)
#         data = response.json()
#         # print(data)
#         # x=0
#         contents = []
#         for item in data:
#             if item != None:
#                 if item['type']!=None:
#                     if item['type'] == 'file':
#                         contents.append(item['download_url'])
#                     elif item['type'] == 'dir':
#                         contents += get_directory_contents(owner, repo, item['path'], token)
#                     else:
#                         continue
#             # x=x+1
#             # if x== 10:
#                 # break
#         # return contents

#         # def get_contents(repo, token):
#         headers = {'Authorization': f'token {token}'}
#         for c in contents:
#             api_url = c
#             response1 = requests.get(api_url, headers=headers)
#             folder=api_url.split('/')[-2]
#             filename=api_url.split('/')[-1]
#             if response1.status_code == 200:
#         # with open('/files/','wb') as file:
#                 file_content=response1.content
#             # print('iteam:'+item)
#             # file_content = requests.get(item['download_url']).content
#                 blob = bucket.blob(folder+'/'+filename)
#                 blob.upload_from_string(file_content)
#                 print(f'Uploaded to Google Cloud Storage')
        
        
#         # print(f'Successfully downloaded file from GitHub: {file_path}')
#         return contents



def list_top_folders_and_files(bucket_name, prefix='', limit=10):
    # storage_client = storage.Client()
    # bucket = storage_client.bucket(bucket_name)

    blobs = bucket.list_blobs(prefix=prefix)

    folders = set()
    files = []
    data=[]
    for blob in blobs:
        name = blob.name

        if name.endswith('/'):
            folder_name = name.split('/')[0]
            if folder_name:
                folders.add(folder_name)
        else:
            files.append(name)

        if len(folders) + len(files) >= limit:
            break

    for folder in folders.copy():
        if len(folders) + len(files) >= limit:
            break
        subfolders, subfiles = list_top_folders_and_files(bucket_name, prefix=folder, limit=limit - (len(folders) + len(files)))
        folders.update(subfolders)
        files.extend(subfiles)

    for f in files:
        x=0
        date_string = f.split('/')[0].split('_')[0]
        date = datetime.strptime(date_string, '%Y%m%d')
        year = date.year
        month = date.month
        day = date.day
        quarter = (month - 1) // 3 + 1
        ticker = f.split('/')[0].split('_')[1]

        url = 'https://raw.githubusercontent.com/plotly/dash-stock-tickers-demo-app/master/tickers.csv'
        df = pd.read_csv(url)

        csymbol = ticker
        filtered_df = df[df['Symbol'] == csymbol]

        if not filtered_df.empty:
            company_name = filtered_df['Company'].iloc[0]
            print(f"Company Name: {company_name}")
        else:
            print("No matching symbol found.")

        stored_url = f"https://storage.cloud.google.com/{bucket_name}/{f}"
        print(f"Stored URL: {stored_url}")
        datatuple= {"'id':"+str(x+1)+", 'name':"+str(company_name)+", 'date':"+str(date)+", 'quarter':"+str(quarter)+", 'month':"+str(month)+", 'day':"+str(day)+", 'year':"+str(year)+", 'ticker':"+str(csymbol)+", 'gurl':"+str(stored_url)}
        
        data.append(datatuple)
        # tup=[str(x+1),str(company_name),str(date),str(quarter),str(month),str(day),str(year),str(csymbol),str(stored_url)]
        # data.append(tup)
        x=x+1
        
    host = '34.74.244.119'
    port = 5432
    database = 'assignment2'
    user = 'postgres'
    password = 'postgres'
    # GOOGLE_APPLICATION_CREDENTIALS="/Users/rishabhindoria/Documents/GitHub/Assignment2/airflow/keys/db_key.json"
    INSTANCE_CONNECTION_NAME="assignment2-390320:us-east1:assignment2"
    # instance_name= 'assignment2-390320:us-east1:assignment2'
    # connection_string = "postgresql+pg8000://{}:{}@{}:{}/{}".format(
    # user, password, host,port,database, project_id=gcs_project_id)
    # engine = create_engine(connection_string)
    # conn = engine.connect()
    # ins = insert('github_import')
    # for row in data:
        # ins.values(id=row[0], name=row[1], date=row[2], quarter=row[3], month=row[4], day=row[5], year=row[6], ticker=row[7], gurl=row[8])
    # conn.execute(ins)
# Data to be inserted
    connector = Connector()

# build connection for db using Python Connector
    
    def getconn():
        conn=connector.connect(
            instance_connection_string=INSTANCE_CONNECTION_NAME,
            driver="pg8000",
            user=user,
            password=password,
            db=database,
        )
        return conn

    pool = create_engine("postgresql+pg8000://", creator = getconn)


    
    with pool.connect() as conn:
        current_time = conn.execute(text("SELECT NOW()")).fetchone()
        print(f"Time: {str(current_time[0])}")


    # def load_data_to_db():
    with pool.connect() as conn:
            # cols = [
            #     (20, 51),    # Name
            #     (72, 75),    # ST
            #     (106, 116),  # Lat
            #     (116, 127)   # Lon
            # ]
        df = pd.DataFrame.from_dict(data)
        # print(df.head())
        df.to_sql(name='github_import', con=conn, index=False, if_exists='replace')
        print("Done")    

    # load_data_to_db

    with pool.connect() as conn:
        df = pd.read_sql_table("github_import", con=conn)
        print(df.head())

# Create an SQLAlchemy engine
#     engine = create_engine(f'postgresql+pg8000://{user}:{password}@{host}:{port}/{database}')

# Define a table using SQLAlchemy's Table construct
#     metadata = MetaData(bind=pool)
#     your_table = Table(
#         'github_import',
#         metadata,
#         Column('id', Integer, primary_key=True),
#         Column('name', String),
#         Column('date', Date),
#         Column('quarter', Integer),
#         Column('month', Integer),
#         Column('day', Integer),
#         Column('year', Integer),
#         Column('ticker', String),
#         Column('gurl', String)
#     )

# # Insert data into the table
#     with pool.connect() as connection:
#     # Insert data using SQLAlchemy's insert() method
#         insert_statement = your_table.insert().values(data)
#         connection.execute(insert_statement)

#     print("Data inserted successfully")
    

with dag:
    # fetch_and_push_task = PythonOperator(
    #     task_id='fetch_and_push_to_gcs',
    #     python_callable=get_directory_contents,
    #     op_args=[owner,repo,directory_path,token]
    # )
    fetch_data_task = PythonOperator(
        task_id='fetch_data',
        python_callable=list_top_folders_and_files,
        op_args=[gcs_bucket_name, '', 10]
    )
    # chain(fetch_and_push_task,fetch_data_task)

