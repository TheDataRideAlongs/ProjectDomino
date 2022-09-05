#!/bin/bash
set -ex

cd ../infra/pipelines/docker/

JOB_NAME="pfas_pfoa_pfos" \
    SEARCH='pfas OR pfoa OR pfos' \
    FETCH_PROFILES="true" \
    START_DATE="2022-01-18 00:00:00" \
    HISTORIC_STRIDE_SEC="`python -c 'print(60 * 60 * 24)'`" \
    TWINT_STRIDE_SEC="`python -c 'print(60 * 60 * 8)'`" \
    STRIDE_SEC="`python -c 'print(30 * 1)'`" \
    WRITE_FORMAT="parquet_s3" \
    S3_FILEPATH="dt-phase1" \
    AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} \
    AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
    ./search_historic.sh $@
