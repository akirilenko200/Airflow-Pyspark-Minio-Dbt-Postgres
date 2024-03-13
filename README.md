# Dockerized open-source data-engineering environment
## Includes Apache Airflow, Apache Spark (Spark Connect), Minio, dbt, Postgres
Comes with scripts to download sample data and pipelines to use them
### Installation:
- Install `docker`
- Read through READMEs in each directory, optionally change `.env` files and run `docker compose` commands. It might be nesessary to change your platform (`arm64` or `amd64`)
- Access Airflow UI at `localhost:8080` and manually trigger pipelines. Look at the output in tasks
- Access Minio UI at `localhost:9001` to look at data