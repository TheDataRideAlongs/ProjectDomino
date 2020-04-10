FROM prefecthq/prefect

RUN pip install supervisor arrow graphistry pyarrow simplejson twarc neo4j
RUN prefect agent install local > supervisord.conf

COPY . .
RUN prefect backend server

RUN date > /dev/null
CMD ["./infra/pipelines/docker/entrypoint.sh"]
