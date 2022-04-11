#step-1
import airflow
from airflow.decorators import task, dag
from datetime import datetime
from airflow.operators.docker_operator import DockerOperator

@dag(start_date=datetime(2018, 1, 3), schedule_interval='@daily', catchup=False)
def oetker_dag():

    @task()
    def t1():
        pass

    t2 = DockerOperator(
        task_id='t2',
        image='moxelpeterle/test_repo1:latest',
        command='python3 main.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge'
    )

    t1() >> t2

dag = oetker_dag()