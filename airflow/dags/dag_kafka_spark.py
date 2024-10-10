import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

# Function to run Kafka Producer
def run_kafka_producer():
    # Run the Kafka producer script
    subprocess.run(["python", "/opt/airflow/dags/kafka_producer.py"])

# Function to run Spark Job
def run_spark_job():
    # Run the Spark job script
    subprocess.run(["python", "/opt/airflow/dags/spark_job.py"])

# Define the default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'catchup': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize the DAG
with DAG(
    dag_id='gps_kafka_spark_pipeline',
    default_args=default_args,
    schedule_interval='@daily',  # Adjust as necessary
    catchup=False,
) as dag:

    # Define the Kafka producer task
    kafka_producer_task = PythonOperator(
        task_id='run_kafka_producer',
        python_callable=run_kafka_producer,
    )

    # Define the Spark job task
    spark_job_task = PythonOperator(
        task_id='run_spark_job',
        python_callable=run_spark_job,
    )

    # Set the task dependencies
    #kafka_producer_task >> spark_job_task