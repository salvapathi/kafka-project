# Connect to MongoDB
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

dag = DAG(
    "Mongodb_mysql",
    default_args={
        "owner": "Salvapathi_Naidu",
        "start_date": airflow.utils.dates.days_ago(1),  # Set the start date to one day ago
    },
    schedule_interval="@daily",
)
    
    



   
# data_extraction = PythonOperator(
#     task_id="Extraction_of_data_From_mongobd",
#     python_callable=data_mongobd,
#     dag=dag,
# )
submit_spark_job = BashOperator(
    task_id='Extraction_of_data_From_mongobd',
    bash_command='spark-submit --master spark://spark-master:7077 --name spark_job --deploy-mode client /opt/airflow/scripts/kafka_scripts/super_bikesscripts.py',
    dag=dag
)

submit_spark_job #>> Data_wrangling 


