#!/bin/bash
set -ex

FILE=${JOB_FILE:-search_by_date_job.py}
PROJECT=${PROJECT_NAME:-docker}


docker-compose -f datastream-docker-compose.yml build # --no-cache
JOB_FILE="search_pfas.py" docker-compose -f datastream-docker-compose.yml -p ${PROJECT} up -d data-stream 
sleep 5
docker-compose -f datastream-docker-compose.yml -p ${PROJECT} logs -f -t --tail=100
