from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.operators.dummy import DummyOperator

import sys
sys.path.append('/opt/airflow/includes')
from emp_dim_insert_update import join_and_detect_new_or_changed_rows
from queries import INSERT_INTO_DWH_EMP_DIM
from queries import UPDATE_DWH_EMP_DIM

def decide_task_to_execute(**kwargs):
    ids_to_update = kwargs['ti'].xcom_pull(task_ids="join_and_detect_new_or_changed_rows", key="ids_to_update")
    if len(ids_to_update)>0:
        return 'update_changed_rows'
    else:
        return 'insert_new_rows'

with DAG("LoadEmpDataFromS3ToSnowflake", start_date=datetime(2023, 4, 23, 0, 0), schedule='@yearly', catchup=False) as dag:
    
    load_emp_sal_to_s3 = SqlToS3Operator(
       task_id="load_emp_sal_to_s3",
        sql_conn_id='ITI_postgres_fatma_connection',
        aws_conn_id='aws_iti_fatma_connection',
        query="SELECT * FROM finance.emp_sal",
        s3_bucket='staging.emp.data',
        s3_key='fatma_emp_sal.csv',
        replace=True
    )
    
    load_emp_details_to_s3 = SqlToS3Operator(
        task_id="load_emp_details_to_s3",
        sql_conn_id='ITI_postgres_fatma_connection',
        aws_conn_id='aws_iti_fatma_connection',
        query="SELECT * FROM hr.emp_details ",
        s3_bucket='staging.emp.data',
        s3_key='fatma_emp_details.csv',
        replace=True
    )

    join_and_detect_changes = join_and_detect_new_or_changed_rows()

    decide_insert_or_update = BranchPythonOperator(
        task_id='decide_insert_or_update',
        python_callable=decide_task_to_execute
    )

    insert_new_rows = SnowflakeOperator(
        task_id='insert_new_rows',
        sql=INSERT_INTO_DWH_EMP_DIM('{{ ti.xcom_pull(task_ids="join_and_detect_new_or_changed_rows", key="rows_to_insert") }}'),
        snowflake_conn_id="snowflake_conn"
    )

    update_changed_rows = SnowflakeOperator(
        task_id='update_changed_rows',
        sql=UPDATE_DWH_EMP_DIM('{{ ti.xcom_pull(task_ids="join_and_detect_new_or_changed_rows", key="ids_to_update") }}'),
        snowflake_conn_id='snowflake_conn'
    )

    insert_new_rows_2 = SnowflakeOperator(
        task_id='insert_new_rows_2',
        sql=INSERT_INTO_DWH_EMP_DIM('{{ ti.xcom_pull(task_ids="join_and_detect_new_or_changed_rows", key="rows_to_insert") }}'),
        snowflake_conn_id='snowflake_conn'
    )

    end_task = BashOperator(task_id='end_task', bash_command='echo "Data load from S3 to Snowflake completed successfully"')
    
    [load_emp_sal_to_s3, load_emp_details_to_s3] >> join_and_detect_changes >> decide_insert_or_update
    decide_insert_or_update >> insert_new_rows >> end_task
    decide_insert_or_update >> update_changed_rows >> insert_new_rows_2 >> end_task
