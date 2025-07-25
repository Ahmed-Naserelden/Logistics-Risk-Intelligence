from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime


# Define the DAG
with DAG(
    dag_id="PortsDAG",
    start_date=datetime(2023, 1, 1),
    schedule_interval="0 0 * * *",  
    catchup=False,
    description="A simple test DAG to print a message",
    tags=["test"],
) as dag:

    # Define a single task
    bronze_layer = BashOperator(
        task_id="Ports_to_Bronze",
        bash_command="ssh -t itversity@itvdelab 'bash -l /home/itversity/spark/bronze/load_bronze.sh'",
    )

    silver_layer = BashOperator(
        task_id="Ports_to_Silver",
        bash_command="ssh itversity@itvdelab /opt/spark3/bin/spark-submit --master local[2] --conf spark.ui.port=18181 /home/itversity/spark/silver/vessels_to_silver.py",
    )


    bronze_layer >> silver_layer

