from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from datetime import datetime
import requests
import logging


# Redshift 연결 설정 함수
def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()


@task
def extract_transform():
    response = requests.get('https://restcountries.com/v3/all')
    countries = response.json()
    records = []
    for country in countries:
        name = country['name']['official']
        population = country['population']
        area = country['area']
        records.append([name, population, area])
    return records

@task
def load(schema, table, records):
    logging.info("load started")
    cur = get_Redshift_connection()
    try:
        cur.execute("BEGIN;")
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
        cur.execute(f"""
                    CREATE TABLE {schema}.{table} (
                        name varchar(150),
                        population int,
                        area float
                    );""")

        for r in records:
            # 국가 명에 ' 포함하는 경우 ''로 치환
            name = r[0].replace("'", "''")
            sql = f"INSERT INTO {schema}.{table} VALUES ('{name}', {r[1]}, {r[2]});"
            print(sql)
            cur.execute(sql)
        cur.execute("COMMIT;")
    except Exception as error:
        print(error)
        cur.execute("ROLLBACK;")
        raise

    logging.info("load done")

with DAG(
    dag_id = 'CountryRestApi',
    start_date = datetime(2023,5,30),
    catchup=False,
    tags=['API'],
    schedule = '30 6 * * 6'
) as dag:
    results = extract_transform()
    load("rlawngh621", "country_info", results)