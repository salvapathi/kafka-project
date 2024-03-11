import os
dependies_path=r"/opt/airflow/scripts" #folder namee
os.chdir(dependies_path)
exec(open(r"/opt/airflow/scripts/python_libraries.py").read())
exec(open(r"/opt/airflow/scripts/credential.py").read()) 
exec(open(r"/opt/airflow/scripts/kafka_scripts/kakfa-producer.py").read()) 
exec(open(r"/opt/airflow/scripts/kafka_scripts/kafka-consumer.py").read()) 
dag = DAG(
    "kafka-monodb",
    default_args={
        "owner": "Salvapathi_Naidu",
        "start_date": airflow.utils.dates.days_ago(1),  # Set the start date to one day ago
    },
    schedule_interval="@daily",
)

# Define the tasks
def kafka():
    kafka_producer("/opt/airflow/data_source/bikes_data.json")
    kafka_consumer('host.docker.internal:9092')
    
   
data_extraction = PythonOperator(
    task_id="kafka_mongobd",
    python_callable=kafka,
    dag=dag,
)

# Data_wrangling = PythonOperator(
#     task_id="Data_wrangling",
#     python_callable=data_wrangling,
#     dag=dag,
# )
# Set the task dependencies
data_extraction #>> Data_wrangling 
