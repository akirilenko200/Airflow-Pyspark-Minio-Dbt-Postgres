import logging
import pendulum

from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.decorators import dag, task

logger = logging.getLogger("airflow.task")

@dag(
        # schedule='@monthly',
        schedule=None,
        start_date=pendulum.datetime(2011,1,1, tz="UTC"),
        catchup=False,
        tags=["dbt"],
        is_paused_upon_creation=True
)
def dbt_dag():
    """
    Dag to run dbt project packaged in a docker image
    The image must be built before
    Config is from Airflow Variables (defined in .env or through UI)
    """

    DockerOperator(
        task_id="run_dbt_models",
        # command="run --vars '{start_date: {{ data_interval_start | ds}}, end_date: {{ data_interval_end | ds}}}'", # incremental run setup
        command="run",
        image="exampledbt:1",
        docker_url="unix://var/run/docker.sock",
        network_mode='examplenet',
        auto_remove=True,
        mount_tmp_dir=False,
        container_name=f"dbt_worker_container",
        environment={ 
            'DBT_HOST' : '{{ var.value.dbt_host }}',
            'DBT_USER' : '{{ var.value.dbt_user}}',
            'DBT_PASSWORD' : '{{ var.value.dbt_password }}',
            'DBT_PORT': '{{ var.value.dbt_port | int }}',
            'DBT_NAME': '{{ var.value.dbt_name }}',
            'DBT_SCHEMA': '{{ var.value.dbt_schema }}',
            }
    )

dbt_dag()
