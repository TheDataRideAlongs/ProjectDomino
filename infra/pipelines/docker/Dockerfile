FROM graphistry/graphistry-blazing:v2.29.3

RUN export DEBIAN_FRONTEND=noninteractive \
    && apt-get update \
    && apt-get install -y --no-install-recommends supervisor \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN pip install prefect==0.10.1 simplejson twarc neo4j
RUN prefect agent install local > supervisord.conf

COPY . .

#TODO find cleaner way to avoid talking to cloud server
RUN prefect backend server

CMD ["./infra/pipelines/docker/entrypoint.sh"]
