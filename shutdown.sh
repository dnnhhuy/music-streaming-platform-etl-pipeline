#!/bin/bash
docker-compose -f docker-compose.yaml -f airflow/docker-compose.yaml \
            -f cassandra/docker-compose.yaml \
            -f hadoop/docker-compose.yaml \
            -f hive/docker-compose.yaml \
            -f presto/docker-compose.yaml \
            -f spark/docker-compose.yaml \
            -f superset/docker-compose.yaml down