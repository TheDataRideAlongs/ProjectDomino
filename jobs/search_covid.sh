#!/bin/bash
set -ex

( cd ../infra/pipelines/docker/ && sudo docker-compose -f datastream-docker-compose.yml -p "covid_tracker" down -v )
( cd ../infra/pipelines/docker/ && sudo docker-compose -f datastream-docker-compose.yml -p "covid_tracker" build )
( cd ../infra/pipelines/docker/ && sudo docker-compose -f datastream-docker-compose.yml -p "covid_tracker" up -d )

