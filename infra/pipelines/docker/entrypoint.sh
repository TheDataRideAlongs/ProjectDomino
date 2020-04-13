#!/bin/bash

# Run prefect agent using supervisord
# supervisord

# Register pipelines
# git fetch && git checkout wzy/dockerizePipelines

python -m pipelines.Pipeline

# Keep the container running
prefect agent start
