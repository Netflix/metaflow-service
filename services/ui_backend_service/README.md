# Metaflow UI Service

Metadata UI service implementation for [Metaflow UI](https://github.com/Netflix/metaflow-ui).
For more information, see [Metaflow's website](http://docs.metaflow.org)

## Getting Started

Refer to the project root for running the project [README.md](../../README.md)

### Metaflow UI deployment

For a complete Metaflow UI production stack deployment (frontend + service + database) please refer to [Cloudformation templates](https://github.com/Netflix/metaflow-tools/tree/master/aws/cloudformation) and [Admin docs](https://admin-docs.metaflow.org/). This is the preferred way to deploy Metaflow UI.

### Running UI Service using the official Docker image

UI service provides backend instance that serves static frontend assets from [Netflix/metaflow-ui](https://github.com/Netflix/metaflow-ui) repository.

UI service is _not_ started by default next to Metadata & Migration services and needs to be started separately.
We strongly recommend deploying the UI service against a [logical replica of the DB](https://aws.amazon.com/blogs/database/using-logical-replication-to-replicate-managed-amazon-rds-for-postgresql-and-amazon-aurora-to-self-managed-postgresql/) that the Metadata service uses to ensure stronger isolation between the UI service and the Metadata service.

The UI service module is `services.ui_backend_service.ui_server`:

> ```sh
> $ /opt/latest/bin/python3 -m services.ui_backend_service.ui_server
> ```

Below is a Docker run command for running UI Service exposed at port 8083:

> ```sh
> $ docker run \
>       -e MF_METADATA_DB_HOST='<instance_name>.us-east-1.rds.amazonaws.com' \
>       -e MF_METADATA_DB_PORT=5432 \
>       -e MF_METADATA_DB_USER='postgres' \
>       -e MF_METADATA_DB_PSWD='postgres' \
>       -e MF_METADATA_DB_NAME='metaflow' \
>       -p 8083:8083 netflixoss/metaflow_metadata_service \
>       /opt/latest/bin/python3 -m services.ui_backend_service.ui_server
> ```

Latest release of the image is available on [dockerhub](https://hub.docker.com/repository/docker/netflixoss/metaflow_metadata_service)

### Hosting the backend

Easiest way to get started is to use `docker-compose`.

Project root has a `docker-compose.yml` that contains a PostgreSQL database as well as the Metadata and UI services.
For development purposes there's also a `docker-compose.development.yml` that will take care of Dockerfile building as well as local volume mounts.

Running the development version (from project root):

> ```sh
> $ docker-compose -f docker-compose.development.yml up
> ```

The service depends on the following Environment Variables to be set:

- `MF_METADATA_DB_HOST` [defaults to localhost]
- `MF_METADATA_DB_PORT` [defaults to 5432]
- `MF_METADATA_DB_USER` [defaults to postgres]
- `MF_METADATA_DB_PSWD` [defaults to postgres]
- `MF_METADATA_DB_NAME` [defaults to postgres]

Optionally you can also override the host and port the service runs on:

- `MF_UI_METADATA_PORT` [defaults to 8083]
- `MF_UI_METADATA_HOST` [defaults to 0.0.0.0]

Running the service without Docker (from project root):

> ```sh
> $ pip3 install -r services/ui_backend_service/requirements.txt
> $ python3 -m services.ui_backend_service.ui_server
> ```

### Hosting the Frontend UI

This service provides the UI Backend. There are two options for hosting the UI Frontend assets from [Metaflow UI](https://github.com/Netflix/metaflow-ui)

#### Separately hosting frontend assets

For hosting the frontend assets for a production environment, refer to the documentation of your chosen host on how to serve static assets.

If you require the UI for local development, refer to [metaflow-ui/docs/README.md](https://github.com/Netflix/metaflow-ui/blob/master/docs/README.md) on how to host the UI locally.

#### Serve frontend assets through the backend instance

Enable built-in UI bundle serving (assumes assets are located inside `ui/` folder):

- `UI_ENABLED` [defaults to `1`]

Use path prefix in case UI service is served under non-root path (`example.com/api/`):

- `PATH_PREFIX=/api` [defaults to `None`]

This also works as a Docker build argument to download and install latest or specific UI release:

> ```sh
> $ docker build --arg UI_ENABLED=1 UI_VERSION=v1.0.0 ...
> ```

Use following environment variables to inject content to Metaflow UI index.html:

- `METAFLOW_HEAD` - Inject content to `head` element
- `METAFLOW_BODY_BEFORE` - Inject content at the beginning of `body` element
- `METAFLOW_BODY_AFTER` - Inject content at the end of `body` element

Use case for these variables ranges from additional meta tags to analytics script injection.

Example on how to add keyword meta tag to Metaflow UI:

```
METAFLOW_HEAD='<meta name="keywords" content="metaflow" />'
```

## Documentation

See [Documentation](docs/README.md) for UI Service specific documentation.
