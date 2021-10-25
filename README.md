# Metaflow Service

Metadata service implementation for [Metaflow](https://github.com/Netflix/metaflow).

This provides a thin wrapper around a database and keeps track of metadata associated with 
metaflow entities such as Flows, Runs, Steps, Tasks, and Artifacts.

For more information, see [Metaflow's admin docs](https://admin-docs.metaflow.org/overview/service-architecture#metadata)

## Getting Started

The service depends on the following Environment Variables to be set:
  - MF_METADATA_DB_HOST [defaults to localhost]
  - MF_METADATA_DB_PORT [defaults to 5432]
  - MF_METADATA_DB_USER [defaults to postgres]
  - MF_METADATA_DB_PSWD [defaults to postgres]
  - MF_METADATA_DB_NAME [defaults to postgres]

Optionally you can also overrider the host and port the service runs on
  - MF_METADATA_PORT [defaults to 8080]
  - MF_MIGRATION_PORT [defaults to 8082]
  - MF_METADATA_HOST [defaults to 0.0.0.0]

Create triggers to broadcast any database changes via `pg_notify` on channel `NOTIFY`:

* `DB_TRIGGER_CREATE`
  * [`metadata_service` defaults to 0]
  * [`ui_backend_service` defaults to 1]

>```sh
>pip3 install ./
>python3 -m services.metadata_service.server
>```

Swagger UI: http://localhost:8080/api/doc

#### Using docker-compose

Easiest way to run this project is to use `docker-compose` and there are two options:

* `docker-compose.yml`
  * Assumes that Dockerfiles are pre-built and local changes are not included automatically
  * See `docker build` section on how to pre-build the Docker images
* `docker-compose.development.yml`
  * Development version
  * Includes automatic Dockerfile builds and mounts local `./services` folder inside the container

Running `docker-compose.yml`:

>```sh
>docker-compose up -d
>```

Running `docker-compose.development.yml` (recommended during development):

>```sh
>docker-compose -f docker-compose.development.yml up
>```

* Metadata service is available at port `:8080`.
* Migration service is available at port `:8082`.
* UI service is available at port `:8083`.

to access the container run

>```sh
>docker exec -it metadata_service /bin/bash
>```

within the container curl the service directly

>```sh
>curl localhost:8080/ping
>```


#### Using published image on DockerHub

Latest release of the image is available on [dockerhub](https://hub.docker.com/repository/docker/netflixoss/metaflow_metadata_service)

>```sh
>docker pull netflixoss/metaflow_metadata_service
>```


Be sure to set the proper env variables when running the image

>```sh
>docker run -e MF_METADATA_DB_HOST='<instance_name>.us-east-1.rds.amazonaws.com' \
>-e MF_METADATA_DB_PORT=5432 \
>-e MF_METADATA_DB_USER='postgres' \
>-e MF_METADATA_DB_PSWD='postgres' \
>-e MF_METADATA_DB_NAME='metaflow' \
>-it -p 8082:8082 -p 8080:8080 metaflow_metadata_service
>```

### Running tests

Tests are run using [Tox](https://tox.readthedocs.io) and [pytest](https://docs.pytest.org).

Run following command to execute tests in Dockerized environment:

> ```sh
> docker-compose -f docker-compose.test.yml up -V --abort-on-container-exit
> ```

Above command will make sure there's PostgreSQL database available.

Usage without Docker:

The test suite requires a PostgreSQL database, along with the following environment variables for connecting the tested services to the DB.

  - MF_METADATA_DB_HOST=db_test
  - MF_METADATA_DB_PORT=5432
  - MF_METADATA_DB_USER=test
  - MF_METADATA_DB_PSWD=test
  - MF_METADATA_DB_NAME=test

> ```sh
> # Run all tests
> tox
>
> # Run unit tests only
> tox -e unit
>
> # Run integration tests only
> tox -e integration
>
> # Run both unit & integrations tests in parallel
> tox -e unit,integration -p
> ```

## Migration Service
The Migration service is a tool to help users manage underlying DB migrations and launch
the most recent compatible version of the metadata service

Note that it is possible to run the two services independently and a Dockerfile is 
supplied for each service. However the default Dockerfile combines the two services.

Also note that at runtime the migration service and the metadata service are completely disjoint and
do not communicate with each other

### Migrating to the latest db schema
Note may need to do a rolling restart to get latest version of the image if you don't have it already

You can manage the migration either via the api provided or with the utility cli provided with `migration_tools.py`

* check status and note version you are on
    * Api: `/db_schema_status`
    * cli: `python3 migration_tools.py db-status`
* see if there are migrations to be run
    * if there are any migrations to be run `is_up_to_date` should be false and a list of migrations to be applied
    will be shown under `unapplied_migrations`
* take backup of db
    * in case anything goes wrong it is a good idea to take a back up of the db
* migrations may cause downtime depending on what is being run as part of the migration
* Note concurrent updates are not supported. it may be advisable to reduce your cluster size to a single node
* upgrade db schema
    * Api: `/upgrade`
    * cli: `python3 migration_tools.py upgrade`
* check status again to verify you are on up to date version
    * Api: `/db_schema_status`
    * cli: `python3 migration_tools.py db-status`
    * Note that `is_up_to_date` should be set to True and `migration_in_progress` should be set to False
* do a rolling restart of the metadata service cluster
    * In order for the migration to be effective a full restart of the containers is required
* latest available version of service should be ready 
    * cli: `python3 migration_tools.py metadata-service-version`
* If you had previously scaled down your cluster it should be safe to return it to the desired number of containers

### Under the Hood: What is going on in the Docker Container 
Within the published metaflow_metadata_service image the migration service is packaged along with 
the latest version of the metadata service compatible with every version of the db. This means that multiple versions
 of the metadata service comes bundled with the image, each is installed under a different virtual env.

When the container spins up, the migration service is launched first and determines what virtualenv to activate
depending on the schema version of the DB. This will determine which version of the metadata service will run.  

## Get in Touch
There are several ways to get in touch with us:

* Open an issue at: https://github.com/Netflix/metaflow-service 
* Email us at: help@metaflow.org
* Chat with us on: http://chat.metaflow.org