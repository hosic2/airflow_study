import requests
from requests.auth import HTTPBasicAuth

# Airflow API 정보
url = "http://localhost:8080/api/v1/dags"
auth = HTTPBasicAuth('airflow', 'airflow')

# 모든 DAG 가져오기
response = requests.get(url, auth=auth)
if response.status_code == 200:
    dags = response.json()["dags"]
    active_dags = [dag for dag in dags if not dag['is_paused']]
    for dag in active_dags:
        print(dag['dag_id'])
else:
    print("Error:", response.status_code, response.text)