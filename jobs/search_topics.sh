#!/bin/bash
set -ex

export JOB_FILE=track_topics.py
export PROJECT=${PROJECT_NAME:-topictracker}

echo "USING: JOB_FILE ${JOB_FILE}, PROJECT ${PROJECT}"


jq -car 'keys[]' topics.json | while read -r topic; do
    ( cd ../infra/pipelines/docker \
      && docker-compose -f datastream-docker-compose.yml -p "${PROJECT}_${topic}" down -v ; )
done




### Run for 30 lines before starting next
N_CHECK=30

jq -car 'keys[]' topics.json | while read -r topic;  do
    echo "topic: ${topic}"
    ( cd ../infra/pipelines/docker \
      && docker-compose -f datastream-docker-compose.yml -p "${PROJECT}_${topic}" build \
      && TOPIC="${topic}" JOB_FILE="${JOB_FILE}" docker-compose -f datastream-docker-compose.yml -p "${PROJECT}_${topic}" up -d \
    && ( docker-compose -f datastream-docker-compose.yml -p "${PROJECT}_${topic}" logs -f -t --tail=$N_CHECK | head -n $N_CHECK; ) ; ) \
    || { echo "exn" && exit 1;  }
done
