#!/bin/bash
set -ex

echo "Starting JOB_FILE ${JOB_FILE}, maybe TOPIC ${TOPIC}"

service tor start

export PYTHONPATH="/modules:${PYTHONPATH}"

python3 /app/${JOB_FILE} $@
