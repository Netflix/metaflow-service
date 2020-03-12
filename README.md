# Metaflow Metadata Service

Metadata service implementation for [Metaflow](https://github.com/Netflix/metaflow).
This provides a thin wrapper around a database and keeps track of metadata associated with 
metaflow entities such as Flows, Runs, Steps, Tasks, and Artifacts.

For more information, see [Metaflow's website](http://docs.metaflow.org)


## Getting Started

The service depends on the following Environment Variables to be set:
  - MF_METADATA_DB_HOST [defaults to localhost]
  - MF_METADATA_DB_PORT [defaults to 5432]
  - MF_METADATA_DB_USER [defaults to postgres]
  - MF_METADATA_DB_PSWD [defaults to postgres]
  - MF_METADATA_DB_NAME [defaults to postgres]

Optionally you can also overrider the host and port the service runs on
  - MF_METADATA_PORT [defaults to 8080]
  - MF_METADATA_HOST [defaults to 0.0.0.0]

>```sh
>pip3 install -r requirements.txt
>python3 -m metadata_service.server
>```

Swagger UI: http://localhost:8080/api/doc

#### Using docker-compose

>```sh
>docker-compose up -d
>```

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
>-e MF_METADATA_DB_NAME='metaflow'
>```

## Get in Touch
There are several ways to get in touch with us:

* Open an issue at: https://github.com/Netflix/metaflow-service 
* Email us at: help@metaflow.org
* Chat with us on: http://chat.metaflow.org
