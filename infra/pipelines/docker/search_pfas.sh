#!/bin/bash
set -ex

FILE=${JOB_FILE:-search_by_date_job.py}
PROJECT=${PROJECT_NAME:-docker}


docker-compose -f datastream-docker-compose.yml down -v
docker-compose -f datastream-docker-compose.yml build # --no-cache
JOB_FILE="search_historic.py" \
DOMINO_JOB_NAME="historic_pfas" \
DOMINO_SEARCH="PFAS" \
DOMINO_START_DATE="2022-01-01 00:00:00" \
DOMINO_STRIDE_SEC="`python -c 'print(30)'`" \
DOMINO_HISTORIC_STRIDE_SEC="`python -c 'print(60 * 60 * 24 * 1)'`" \
DOMINO_TWINT_STRIDE_SEC="`python -c 'print(60 * 60 * 8)'`" \
    docker-compose -f datastream-docker-compose.yml \
        -p ${PROJECT} \
        up -d --force-recreate data-stream 
sleep 5
docker-compose -f datastream-docker-compose.yml -p ${PROJECT} logs -f -t --tail=100
