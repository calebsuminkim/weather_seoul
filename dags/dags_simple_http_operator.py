from http.client import responses

import pendulum
from airflow.providers.http.operators.http import HttpOperator
from airflow.sdk import DAG, task

with DAG(
    dag_id='dags_simple_http_operator',
    start_date=pendulum.datetime(2025, 11,24, tz='Asia/Seoul'),
    catchup=False,
    schedule=None
) as dag:
    import json
    s_dot_info = HttpOperator(
        task_id='s_dot_info',
        http_conn_id='seoul_public_data_http',
        endpoint='{{ var.value.apikey_openapi_seoul_go_kr }}/json/IotVdata017/1/20',
        method='GET',
        headers={
            'Content-Type': 'application/json',
            'charset': 'utf-8',
            'Accept': '*/*'
        },
        response_filter=lambda response: response.json(),
        log_response=True
    )

    # @task(task_id='python_2') # s_dot_info(HTTPOperator)가 리턴한 값을 가지고 오는 태스크
    # def python_2(**kwargs):
    #     ti = kwargs['ti']
    #     rslt = ti.xcom_pull(task_ids='s_dot_info')
    #
    #     from pprint import pprint
    #
    #     pprint(json.loads(rslt))

    s_dot_info