prefect-up :
	export APOLLO_HOST_PORT=4200 \
	GRAPHQL_HOST_PORT=4201 \
	HASURA_HOST_PORT=3000 \
	POSTGRES_HOST_PORT=5432 \
	UI_HOST_PORT=8080 \
	POSTGRES_USER=prefect \
	POSTGRES_PASSWORD=test-password \
	POSTGRES_DB=prefect_server && \
	export DB_CONNECTION_URL=postgresql://$$POSTGRES_USER:$$POSTGRES_PASSWORD@postgres:$$POSTGRES_HOST_PORT/$$POSTGRES_DB \
	HASURA_API_URL=http://hasura:$$HASURA_HOST_PORT/v1alpha1/graphql \
	HASURA_WS_URL=ws://hasura:$$HASURA_HOST_PORT/v1alpha1/graphql \
	PREFECT_API_HEALTH_URL=http://graphql:$$GRAPHQL_HOST_PORT/health \
	PREFECT_API_URL=http://graphql:$$GRAPHQL_HOST_PORT/graphql/ \
	PREFECT_SERVER_DB_CMD='prefect-server database upgrade -y' && \
	docker-compose -f ../infra/prefect/docker/docker-compose.yml up

agent :
	cd .. && \
	prefect backend server && \
	python -m pipelines.Pipeline && \
	prefect agent start

agent-docker :
	cd .. && \
	docker run -e PREFECT__SERVER__HOST=http://host.docker.internal \
	-e PREFECT__SERVER__PORT=4200 \
	$$(docker build -f infra/pipelines/docker/Dockerfile -q .)
