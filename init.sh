#!/bin/bash

# Debug Java installation
echo "Checking Java installation..."
echo "JAVA_HOME=$JAVA_HOME"
ls -l $JAVA_HOME/bin/java || echo "Java binary not found at expected location"
which java
java -version

echo "Waiting for postgres..."
while ! nc -z postgres 5432; do
    sleep 1
done
echo "PostgreSQL started"

echo "Running database migrations..."
airflow db migrate

echo "Creating admin user..."
airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@example.com \
    --password admin

echo "Starting Airflow webserver and scheduler..."
airflow webserver & airflow scheduler