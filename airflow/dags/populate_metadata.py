# from airflow.models import DAG
# from airflow.operators.bash_operator import BashOperator
# from airflow.operators.python_operator import PythonOperator
# from airflow.utils.dates import days_ago
# from airflow.models.param import Param
# from datetime import timedelta
# from airflow.models.baseoperator import chain
# import requests
# import csv
# import json
# import os

# dag = DAG(
#     dag_id="metadata_load",
#     # Run daily midnight to fetch metadata from github
#     schedule="0 0 * * *",   # https://crontab.guru/
#     start_date=days_ago(0),
#     catchup=False,
#     dagrun_timeout=timedelta(minutes=60),
#     tags=["assignment1", "damg7245"],
# )


# def get_all_company_name_tickers(ti):
#     url = "https://raw.githubusercontent.com/plotly/dash-stock-tickers-demo-app/master/tickers.csv"
#     response = requests.get(url)
#     text_data = response.text
#     # Create an empty dictionary
#     stock_tickers = {}
#     # Parse the CSV data and populate the dictionary
#     csv_reader = csv.reader(text_data.splitlines())
#     header = next(csv_reader)
#     for row in csv_reader:
#         name, symbol = row
#         stock_tickers[symbol] = name
#     # Print the resulting dictionary
#     print(list(stock_tickers.keys()))
#     ti.xcom_push(key="company_tickers", value=list(stock_tickers.keys()))
#     return list(stock_tickers.keys())

# def get_directories():
#     directories = []
#     # Define the repository URL
#     repo_url = 'https://api.github.com/repos/Earnings-Call-Dataset/MAEC-A-Multimodal-Aligned-Earnings-Conference-Call-Dataset-for-Financial-Risk-Prediction/contents/MAEC_Dataset'

#     # Send a GET request to the repository URL
#     response = requests.get(repo_url)

#     # Check if the request was successful
#     if response.status_code == 200:
#     # Parse the JSON response
#         data = response.json()

#     # Iterate over each item in the response
#         for item in data:
#         # Check if the item is a directory
#             if item['type'] == 'dir':
#                 directories.append(item['html_url'].split('/')[-1])
#             # Print the folder name
#             # print(item['name'])
#         print(directories)
#         return directories
#     else:
#       # If the request was not successful, print the status code
#         print('Failed to retrieve folder names. Status Code:', response.status_code)
#         print(directories)
#         return directories

# def get_company_names_with_transcripts(ti):
#     directories = get_directories()
#     parsed_directories = [dir.split("_") for dir in directories]
#     company_names = [dir[-1] for dir in parsed_directories]
#     myxcom_val = ti.xcom_pull(key="return_value", task_ids='get_all_company_name_tickers')
#     ti.xcom_push(key="ret_tickers", value=myxcom_val)
#     ti.xcom_push(key="parsed_directories", value=parsed_directories)
#     return parsed_directories

# def extract_names_years(ti):
#     company_tickers = ti.xcom_pull(key="return_value", task_ids='get_all_company_name_tickers')
#     parsed_directories = ti.xcom_pull(key="return_value", task_ids='get_company_names_with_transcripts')
    
#     company_names = [dir[-1] for dir in parsed_directories]
#     # common_company_names = [set(company_tickers).intersection(set(company_names))]
    
#     company_names_with_years = {}
#     for dir in parsed_directories:
#         name, date = dir[-1], dir[0]
#         date = date[:4]
#         if name in company_names_with_years:
#             company_names_with_years[name].add(int(date))
#         else:
#             company_names_with_years[name] = set([int(date)])
#     company_names_with_years = {name:list(years) for name, years in company_names_with_years.items()}
#     return company_names_with_years

# def store_metadata_postgres(ti):
#     company_names_with_years = ti.xcom_pull(key="return_value", task_ids='extract_names_years')
#     url = f"{os.getenv('BACKEND_API_URL')}/api/v1/company_metadata/store"
#     json_payload = json.dumps({"company_names_years": company_names_with_years})
#     ti.xcom_push(key="company_names_years", value=json_payload)
#     headers = {'Content-Type': 'application/json'}
#     response = requests.request("POST", url, headers=headers, data=json_payload)
#     return response.text

# with dag:

#     get_data_from_github = PythonOperator(
#         task_id='get_all_company_name_tickers',
#         python_callable=get_all_company_name_tickers,
#         provide_context=True,
#         dag=dag,
#     )

#     get_company_names_with = PythonOperator(
#         task_id='get_company_names_with_transcripts',
#         python_callable=get_company_names_with_transcripts,
#         provide_context=True,
#         dag=dag,
#     )

#     extract_names_years_task = PythonOperator(
#         task_id='extract_names_years',
#         python_callable=extract_names_years,
#         provide_context=True,
#         dag=dag,
#     )
    
#     store_metadata_postgres_task = PythonOperator(
#         task_id='store_metadata_postgres',
#         python_callable=store_metadata_postgres,
#         provide_context=True,
#         dag=dag,
#     )
#     # Flow
#     chain([get_data_from_github, get_company_names_with],extract_names_years_task, store_metadata_postgres_task)
    