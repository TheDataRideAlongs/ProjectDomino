#!/bin/bash

echo "Starting prefect executor daemon in foreground"
source activate rapids && supervisord --nodaemon