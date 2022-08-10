#!/usr/bin/bash

echo "Setting up of Running Airflow in Docker..."
echo "Download docker-compose yaml file.."
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.3.3/docker-compose.yaml'
echo "creating the requisite directories"

mkdir -p ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)" > .env
AIRFLOW_UID=50000
echo "Initialize the database"
docker-compose up airflow-init
echo "Starting the airflow"
sleep 5
docker-compose up