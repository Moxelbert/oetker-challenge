#step-1
import airflow
from airflow.decorators import task, dag
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.operators.docker_operator import DockerOperator

@dag(start_date=datetime(2018, 1, 3), schedule_interval='@daily', catchup=False)
def oetker_dag():

    t1 = DockerOperator(
        task_id='t2',
        image='moxelpeterle/test_repo1:latest',
        command='python3 src/main.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge'
    )

    t1()

dag = oetker_dag()