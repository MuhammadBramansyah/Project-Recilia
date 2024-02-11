# Project-Recilia
This repository contains the code in my local machine for working this project

## How To Run
run docker build:
```
docker build . --tag extending_airflow:latest
```
## Change Docker Image in Docker-Compose.yaml
```
image: ${AIRFLOW_IMAGE_NAME:-customed-airflow:latest}
```
## Run Docker Compose:
```
docker-compose up -d
```

