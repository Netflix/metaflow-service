# Metaflow UI Service

Metadata UI service implementation for [Metaflow](https://github.com/Netflix/metaflow-ui).
For more information, see [Metaflow's website](http://docs.metaflow.org)

TODO: This UI Service might introduce some overlap between the Metadata Service, in future it might make sense to combine some of these API endpoints.

## Getting Started

Refer to project root for running the project [README.md](../../README.md)

Easiest way to get started is to use `docker-compose`.

Project root has `docker-compose.yml` that contains PostgreSQL database as well as Metadata service and UI service.
For development purposes there's also `docker-compose.development.yml` that will take care of Dockerfile building as well as local volume mounts.

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

Optionally you can also overrider the host and port the service runs on:

- `MF_UI_METADATA_PORT` [defaults to 8083]
- `MF_UI_METADATA_HOST` [defaults to 0.0.0.0]

Enable built-in UI bundle serving (assumes assets are located inside `ui/` folder):

- `UI_ENABLED` [defaults to 0]

Use path prefix in case UI service is served under non-root path (`example.com/api/`):

- `PATH_PREFIX=/api` [defaults to None]

This also works as a Docker build argument to download and install latest or specific UI release:

> ```sh
> $ docker build --arg UI_ENABLED=1 UI_VERSION=v0.1.2 ...
> ```

Configure amount of seconds realtime events are kept in queue (delivered to UI in case of reconnects):

- `WS_QUEUE_TTL_SECONDS` [defaults to 300 (5 minutes)]

Configure amount of runs to prefetch during server startup (artifact cache):

- `PREFETCH_RUNS_SINCE` [in seconds, defaults to 2 days ago (86400 * 2 seconds)]
- `PREFETCH_RUNS_LIMIT` [defaults to 50]

Configure the amount of concurrent cache actions. This works similar to a database connection pool.

- `CACHE_ARTIFACT_MAX_ACTIONS` [max number of artifact cache actions. Defaults to 16]
- `CACHE_DAG_MAX_ACTIONS` [max number of DAG cache actions. Defaults to 16]

Configure the maximum usable space by the cache:

- `CACHE_ARTIFACT_STORAGE_LIMIT` [in bytes, defaults to 600000]
- `CACHE_DAG_STORAGE_LIMIT` [in bytes, defaults to 100000]

Running the service without Docker (from project root):

> ```sh
> $ pip3 install -r services/ui_backend_service/requirements.txt
> $ python3 -m services.ui_backend_service.ui_server
> ```

## Baseurl configuration

Use `MF_BASEURL` environment variable to overwrite the default API baseurl.
This affects API responses where meta links are provided as a response.

## API examples

```
/flows/HelloFlow/runs?_page=4                               List page 4
/flows/HelloFlow/runs?_page=2&_limit=10                     List page 4, each page contains 10 items

/flows/HelloFlow/runs?_order=run_number                     Order by `run_number` in descending order
/flows/HelloFlow/runs?_order=+run_number                    Order by `run_number` in ascending order
/flows/HelloFlow/runs?_order=-run_number                    Order by `run_number` in descending order
/flows/HelloFlow/runs?_order=run_number,ts_epoch            Order by `run_number` and `ts_epoch` in descending order

/runs?_tags=user:dipper                                     Filter by one tag
/runs?_tags=user:dipper,runtime:dev                         Filter by multiple tags (AND)
/runs?_tags:all=user:dipper,runtime:dev                     Filter by multiple tags (AND)
/runs?_tags:any=user:dipper,runtime:dev                     Filter by multiple tags (OR)
/runs?_tags:likeall=user:dip,untime:de                      Filter by multiple tags that contains string (AND)
/runs?_tags:likeany=user:,untime:de                         Filter by multiple tags that contains string (OR)

/runs?_group=flow_id                                        Group by `flow_id`
/runs?_group=flow_id,user_name                              Group by `flow_id` and `user_name`
/runs?_group=user_name&_limit=2                             Group by `user_name` and limit each group to `2` runs
/runs?_group=flow_id&_order=flow_id,run_number              Group by `flow_id` and order by `flow_id & run_number`
/runs?_group=flow_id&user_name=dipper                       List runs by `dipper` and group by `flow_id`

/flows/HelloFlow/runs?run_number=40                         `run_number` equals `40`
/flows/HelloFlow/runs?run_number:eq=40                      `run_number` equals `40`
/flows/HelloFlow/runs?run_number:ne=40                      `run_number` not equals `40`
/flows/HelloFlow/runs?run_number:lt=40                      `run_number` less than `40`
/flows/HelloFlow/runs?run_number:le=40                      `run_number` less than or equals `40`
/flows/HelloFlow/runs?run_number:gt=40                      `run_number` greater than `40`
/flows/HelloFlow/runs?run_number:ge=40                      `run_number` greater than equals `40`

/flows/HelloFlow/runs?user_name:co=atia                     `user_name` contains `atia`
/flows/HelloFlow/runs?user_name:sw=mati                     `user_name` starts with `mati`
/flows/HelloFlow/runs?user_name:ew=tias                     `user_name` ends with `tias`

/flows?user_name=dipper,mabel                               `user_name` is either `dipper` OR `mabel`

/flows/HelloFlow/runs?run_number:lt=60&run_number:gt=40     `run_number` less than 60 and greater than 40
```

## Available operators

```
eq = equals                 =
ne = not equals             !=
lt = less than              <
le = less than equals       <=
gt = greater than           >
ge = greater than ewquals   >=
co = contains               *string*
sw = starts with            ^string*
ew = ends with              *string$
```

## Custom Navigation links for UI

You can customize the admin navigation links presented by the UI by renaming the provided `links.example.json` file to `links.json`. The backend service serves the contents of the file as-is, so be sure to adhere to the predefined structure.

**Local Dev**
Simply edit the `links.json` file content

**Prebuilt docker image**
You can mount a custom `links.json` when launching the Docker container with a command such as

```bash
  docker run -v /path/to/custom/links.json:/root/service/ui_backend_service/links.json metaflow/ui-service
```
