ARG build_for=linux/amd64

FROM --platform=${build_for} apache/airflow:2.8.1
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.1/constraints-3.8.txt"