import requests
from datetime import datetime as dt
from bs4 import BeautifulSoup

from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

default_args = {
    'owner': 'airflow',
    'start_date': dt(2024, 7, 1)
}

def page_scrap():
    i = 0
    rows = []
    while i < 25:
        i += 1
        url = f'https://tanba.kezekte.kz/ru/frameless/animal/list?p={i}'

        response = requests.get(url, verify=False)
        soup = BeautifulSoup(response.content, 'html.parser')

        for tr in soup.find_all('tr')[1:]:
            cells = []
            tds = tr.find_all('td')
            for td in tds:
                cells.append(td.text.strip())
            rows.append(cells)
        print(f'page {i}')

    return rows
    
def transform_data(task_instance):
    datasets = task_instance.xcom_pull(task_ids='page_scrap')
    transformed_data = []
    for data in datasets:
        animal_id = data[0]
        animal_type = data[1]

        animal_nickname = data[2][0].upper() + data[2][1:].lower()
        if animal_nickname == '-' or animal_nickname == 'Нет' or \
           animal_nickname == 'Не имеется' or animal_nickname == 'Без клички':
            animal_nickname = None
        elif animal_nickname.isdigit() and len(animal_nickname) == 4:
            animal_nickname = 'Number_nickname'

        animal_gender = data[3]
        if animal_gender.lower() == 'самец':
            animal_gender = 'Male'
        else:
            animal_gender = 'Female'

        animal_breed = data[4]
        animal_passport = data[5]

        animal_register = dt.strptime(data[6], '%d.%m.%Y %H:%M:%S')
        animal_birthday = dt.strptime(data[7], '%d.%m.%Y').date()
        
        animal_status = data[8]
        animal_ownership = data[9]
        animal_register_type = data[10]
        animal_tagging_location = data[11]
        animal_tagging_place = data[12]

        animal_tagging_date = dt.strptime(data[13], '%d.%m.%Y').date()

        animal_district_area = data[14]
        animal_date_vaccination = data[15]

        animal_date_sterilization_castration = data[16]
        if animal_date_sterilization_castration == 'Не стерилизован/кастрирован':
            animal_date_sterilization_castration = None
        else:
            animal_date_sterilization_castration = dt.strptime(data[16], '%d.%m.%Y').date()

        transformed_data.append((
            animal_id,
            animal_type,
            animal_nickname,
            animal_gender,
            animal_breed,
            animal_passport,
            animal_register,
            animal_birthday,
            animal_status,
            animal_ownership,
            animal_register_type,
            animal_tagging_location,
            animal_tagging_place,
            animal_tagging_date,
            animal_district_area,
            animal_date_vaccination,
            animal_date_sterilization_castration
        ))
    
    task_instance.xcom_push(key='transformed_data', value=transformed_data)

def transformed_data_to_psdb(task_instance):
    postgres_hook = PostgresHook(postgres_conn_id='postgresql_conn')
    transformed_data = task_instance.xcom_pull(task_ids='transform_data', key='transformed_data')
    
    insert_sql = """
        insert into transformed_animals (chip_id, type, nickname, gender, breed, passport_number, register_date, birthday, status, ownership, 
                                register_type, tagging_location, tagging_place, tagging_date, district_area, date_vaccination, date_sterilization_castration)
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        on conflict (passport_number) do nothing;
    """

    for data in transformed_data:
        postgres_hook.run(insert_sql, parameters=data)

with DAG(
    dag_id='kezekte_etl',
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False
) as dag:

    page_scrap = PythonOperator(
        task_id='page_scrap',
        python_callable=page_scrap
    )

    create_table = SQLExecuteQueryOperator(
        task_id='create_table',
        conn_id='postgresql_conn',
        sql="""
            create table if not exists transformed_animals (
                id serial PRIMARY KEY,
                chip_id varchar(255),
                type varchar(255),
                nickname varchar(255),
                gender varchar(255),
                breed varchar(255),
                passport_number varchar(255),
                register_date timestamp,
                birthday date,
                status varchar(255),
                ownership varchar(255),
                register_type varchar(255),
                tagging_location varchar(255),
                tagging_place varchar(255),
                tagging_date date,
                district_area varchar(255),
                date_vaccination varchar(255),
                date_sterilization_castration date,
                CONSTRAINT unique_passport unique (passport_number)
            )
        """
    )

    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )

    transformed_data_to_psdb = PythonOperator(
        task_id='transformed_data_to_psdb',
        python_callable=transformed_data_to_psdb
    )

    page_scrap >> create_table >> transform_data >> transformed_data_to_psdb
