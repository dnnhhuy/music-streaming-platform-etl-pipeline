#!/bin/bash
docker build -t airflow-spark ./airflow/
docker build -t eventsim ./docker_eventsim/
docker build -t superset ./superset/

docker-compose -f docker-compose.yaml -f airflow/docker-compose.yaml \
            -f cassandra/docker-compose.yaml \
            -f hadoop/docker-compose.yaml \
            -f hive/docker-compose.yaml \
            -f presto/docker-compose.yaml \
            -f spark/docker-compose.yaml \
            -f superset/docker-compose.yaml up -d 
