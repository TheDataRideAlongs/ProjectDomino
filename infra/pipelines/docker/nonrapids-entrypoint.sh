#!/bin/bash

echo "Starting prefect executor daemon in foreground"
supervisord --nodaemon
