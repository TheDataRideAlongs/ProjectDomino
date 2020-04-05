# Pipelines

This folder contains the data ingestion pipelines. It uses [Prefect](http://prefect.io) as the orchestrator.

To run the pipelines locally, we need to start the Prefect UI + server container by running `make prefect-up`. Wait for the message (it should take a few seconds)

```
Server ready at http://0.0.0.0:4200/ 🚀
```

You can then access the Prefect UI at http://localhost:8080.

To deploy the pipelines to Prefect, we need to run the Prefect agent (which registers flows to the Prefect server and polls for flow runs). We can either use (in a separate CLI):

1. `make agent` (preferred for development): this script runs the Prefect agent/pipelines locally.
2. `make agent-docker` (sanity check before merging changes): this script packs a Prefect agent and this repository into a docker container, and runs them inside the container. It takes a while to build the container, but it is also what we use in production, so before merging changes into master it's a great sanity check.
