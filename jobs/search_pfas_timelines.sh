#!/bin/bash

USERNAMES=""
while IFS= read -r LINE; do
   USERNAMES="$USERNAMES,$LINE"
done < pfas_profiles

echo "Users: $USERNAMES"

#set -ex

cd ../infra/pipelines/docker/

JOB_NAME="pfas_timelines" \
    FETCH_PROFILES="true" \
    USERNAMES="$USERNAMES" \
    STRIDE_SEC="`python -c 'print(30 * 1)'`" \
    WRITE_FORMAT="parquet_s3" \
    S3_FILEPATH="dt-phase1" \
    AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} \
    AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
    ./search_timelines.sh $@
