from airflow import DAG
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from create_conn import create_essential_conn



default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': days_ago(1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag =  DAG(
    dag_id='testing_dag',
    default_args=default_args,
    max_active_runs=1,
	schedule_interval="*/30 * * * *",
    catchup=False)
    
op0 = PythonOperator(
    task_id="create_connection",
    python_callable=create_essential_conn,
    dag=dag
)

op1 = SparkSubmitOperator(
    conn_id="spark_conn",
    task_id= "batch_etl",
    application="dags/batch_etl.py",
    verbose=True,
    dag=dag
)

op2 = BashOperator(
    task_id="create_database_tables",
    bash_command="beeline -u {{params.url}} -f {{params.script}}",
    params={"url": "jdbc:hive2://hive-server2:10000",
            "script": "/opt/airflow/dags/hql/create_database_tables.hql"},
    dag=dag
)

start = EmptyOperator(task_id="start", dag=dag)
end = EmptyOperator(task_id="end", dag=dag)  
start  >> op0 >> op1 >> op2 >> end

if __name__ == "__main__":
    dag.cli()
