#!/bin/bash
set -ex

service tor start

python3 /app/${JOB_FILE} $@
