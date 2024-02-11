FROM apache/airflow:2.3.2
USER root
RUN apt-get update \
    && apt-get install -y vim 
USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /requirements.txt
