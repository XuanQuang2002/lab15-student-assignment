from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineer',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['data_team@example.com'],
}

def print_execution_report(**kwargs):
    """
    Generate a summary report of the pipeline execution.
    """
    print("="*50)
    print(" 🚀 SPARK E-COMMERCE ETL PIPELINE EXECUTION REPORT 🚀")
    print("="*50)
    print(f"DAG Run ID: {kwargs['dag_run'].run_id}")
    print(f"Logical Date: {kwargs['logical_date']}")
    print("Status: SUCCESS ✅")
    print("Pipeline has successfully extracted Data Lake files,")
    print("transformed data to Silver layer,")
    print("and loaded into PostgreSQL Gold layer using PySpark.")
    print("="*50)

with DAG(
    'ecommerce_etl_spark',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='End-to-End ETL for E-commerce Data using Apache Spark'
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    t_extract = SparkSubmitOperator(
        task_id='extract_to_bronze',
        application='/opt/airflow/src/spark_etl.py',
        conn_id='spark_default',
        application_args=['--layer', 'bronze'],
        name='ecommerce_extract_bronze'
    )
    
    t_transform = SparkSubmitOperator(
        task_id='transform_to_silver',
        application='/opt/airflow/src/spark_etl.py',
        conn_id='spark_default',
        application_args=['--layer', 'silver'],
        name='ecommerce_transform_silver'
    )
    
    t_data_quality = SparkSubmitOperator(
        task_id='data_quality_check',
        application='/opt/airflow/src/spark_etl.py',
        conn_id='spark_default',
        application_args=['--layer', 'data_quality'],
        name='ecommerce_data_quality'
    )
    
    t_load = SparkSubmitOperator(
        task_id='load_to_gold',
        application='/opt/airflow/src/spark_etl.py',
        conn_id='spark_default',
        application_args=['--layer', 'gold'],
        packages='org.postgresql:postgresql:42.5.4',
        name='ecommerce_load_gold'
    )
    
    t_report = PythonOperator(
        task_id='execution_report',
        python_callable=print_execution_report
    )
    
    end = EmptyOperator(
        task_id='end',
        trigger_rule='none_failed'
    )
    
    start >> t_extract >> t_transform >> t_data_quality >> t_load >> t_report >> end
