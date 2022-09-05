#!/bin/bash
set -ex

cd ../infra/pipelines/docker/

JOB_NAME="pfas_pfoa_pfos" \
    SEARCH='pfas OR pfoa OR pfos' \
    START_DATE="2022-01-01 00:00:00" \
    HISTORIC_STRIDE_SEC="`python -c 'print(60 * 60 * 24)'`" \
    TWINT_STRIDE_SEC="`python -c 'print(60 * 60 * 8)'`" \
    STRIDE_SEC="`python -c 'print(30 * 1)'`" \
    WRITE_FORMAT="parquet" \
    ./search_historic.sh $@
