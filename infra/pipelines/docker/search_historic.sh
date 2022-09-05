#!/bin/bash
set -ex

DOMINO_JOB_NAME=${JOB_NAME:-historic_pfas_1}
DOMINO_SEARCH=${SEARCH:-"pfas"}
DOMINO_FETCH_PROFILES=${FETCH_PROFILES:-"false"}
DOMINO_START_DATE=${START_DATE:-2022-01-01 00:00:00}
DOMINO_STRIDE_SEC=${STRIDE_SEC:-30}
DOMINO_HISTORIC_STRIDE_SEC=${HISTORIC_STRIDE_SEC:-86400}
DOMINO_TWINT_STRIDE_SEC=${TWINT_STRIDE_SEC:-28800}
DOMINO_WRITE_FORMAT=${WRITE_FORMAT:-parquet}
DOMINO_S3_FILEPATH=${S3_FILEPATH:-dt-phase1}
DOMINO_COMPRESSION=${COMPRESSION:-snappy}
AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID:-}
AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY:-}

docker-compose -f datastream-docker-compose.yml -p ${DOMINO_JOB_NAME} down -v
docker-compose -f datastream-docker-compose.yml build  # --no-cache
JOB_FILE="search_historic.py" \
    DOMINO_JOB_NAME=$DOMINO_JOB_NAME \
    DOMINO_SEARCH=$DOMINO_SEARCH \
    DOMINO_FETCH_PROFILES=$DOMINO_FETCH_PROFILES \
    DOMINO_START_DATE=$DOMINO_START_DATE \
    DOMINO_STRIDE_SEC=$DOMINO_STRIDE_SEC \
    DOMINO_HISTORIC_STRIDE_SEC=$DOMINO_HISTORIC_STRIDE_SEC \
    DOMINO_TWINT_STRIDE_SEC=$TWINT_STRIDE_SEC \
    DOMINO_WRITE_FORMAT=$DOMINO_WRITE_FORMAT \
    DOMINO_S3_FILEPATH=$DOMINO_S3_FILEPATH \
    DOMINO_COMPRESSION=$DOMINO_COMPRESSION \
    AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
    AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
    docker-compose -f datastream-docker-compose.yml \
        -p ${DOMINO_JOB_NAME} \
        up \
        data-stream \
        $@