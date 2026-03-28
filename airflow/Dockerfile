FROM apache/airflow:2.10.5-python3.12

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /requirements-project.txt
COPY airflow/requirements.txt /requirements-airflow.txt
RUN pip install --no-cache-dir -r /requirements-airflow.txt
