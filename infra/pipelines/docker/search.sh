#!/bin/bash

docker-compose -f datastream-docker-compose.yml build && docker-compose -f datastream-docker-compose.yml up -d && docker-compose -f datastream-docker-compose.yml logs -f -t --tail=1
