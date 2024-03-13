# Local Environment Setup

## Apache Airflow
- Official [docker-compose](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html) with relabled default bridge network and some changes. Provides interoperability with other services created using another docker-compose using their names and not localhost loopback.

- Using extended image: Check [Dockerfile](pipeline/airflow/Dockerfile), [requirements.txt](pipeline/airflow/requirements.txt) and platform in [docker-compose.yml](pipeline/airflow/docker-compose.yml) 
- Run `docker compose up -d`. Also creates a user `airflow` with password `airflow`
- Start and stop with `docker compose start/stop`
- Rebuild and `docker compose up -d` will pick up the changes
- Uninstall with `docker compose down --volumes --remove-orphans` and `sudo rm -rf ./dags/ ./logs/ ./plugins/`

- Web UI is available at http://localhost:8080
- Locally `pip install -r requirements.txt --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.1/constraints-3.8.txt"` (match versions)

- To quickly refresh DAGs `docker exec -it airflow-scheduler bash -c "airflow dags list"`
- Backfill `docker exec -it airflow-scheduler bash -c "airflow dags backfill --start-date 2023-1-16 --end-date 2023-1-18 test_dag"`

- Connections and credentials are in environment variables