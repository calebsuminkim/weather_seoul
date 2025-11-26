# from operators.seoul_api_to_csv_operator import SeoulApiToCsvOperator
from operators.seoul_api_to_csv_operator import SeoulApiToCsvOperator
from airflow import DAG
import pendulum

with DAG(
        dag_id='dags_seoul_api_s_dot',
        schedule='0 7 * * *',
        start_date=pendulum.datetime(2025, 11, 23, tz='Asia/Seoul'),
        catchup=False
) as dag:
    '''s_dot'''
    s_dot_operator = SeoulApiToCsvOperator(
        task_id='s_dot_operator',
        dataset_nm='IotVdata017',
        path='/opt/airflow/files/IotVdata017/{{ data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}',
        file_name='IotVdata017.csv'
    )

    s_dot_gangnam = SeoulApiToCsvOperator(
        task_id='s_dot_gangnam',
        dataset_nm='IotVdata017',
        path='/opt/airflow/files/IotVdata017/Yeoksam/{{ data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}',
        file_name='{{ data_interval_end.in_timezone("Asia/Seoul") }}_Gangnam.csv',
        base_dt='Gangnam-gu'
    )

    s_dot_operator >> s_dot_gangnam