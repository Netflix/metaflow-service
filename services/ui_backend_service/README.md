# Metaflow UI Service

Metadata UI service implementation for [Metaflow](https://github.com/Netflix/metaflow-ui).
For more information, see [Metaflow's website](http://docs.metaflow.org)

TODO: This UI Service might introduce some overlap between the Metadata Service, in future it might make sense to combine some of these API endpoints.

## Getting Started

Refer to project root for running the project [README.md](../../README.md)

### Hosting the backend

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


Running the service without Docker (from project root):

> ```sh
> $ pip3 install -r services/ui_backend_service/requirements.txt
> $ python3 -m services.ui_backend_service.ui_server
> ```
### Hosting the Frontend UI

This service provides the UI Backend. There are two options for hosting the UI Frontend assets from [Metaflow-ui](url-to-be-added)

#### Separately hosting frontend assets

For hosting the frontend assets for a production environment, refer to the documentation of your chosen host on how to serve static assets.

If you require the UI for local development, refer to [Metaflow-ui/README.md](url-to-be-added) on how to host the UI locally.

#### Serve frontend assets through the backend instance 

Enable built-in UI bundle serving (assumes assets are located inside `ui/` folder):

- `UI_ENABLED` [defaults to 0]

Use path prefix in case UI service is served under non-root path (`example.com/api/`):

- `PATH_PREFIX=/api` [defaults to None]

This also works as a Docker build argument to download and install latest or specific UI release:

> ```sh
> $ docker build --arg UI_ENABLED=1 UI_VERSION=v0.1.2 ...
> ```

## API documentation

A thorough documentation of API routes, responses and types can be accessed through the Swagger docs that the backend serves.
These are accessible at `example.com/api/doc`
### Examples

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
/runs?user=null                                             `user` is NULL

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

### Available operators

| URL operator | Description             | SQL operator |
|--------------|-------------------------|--------------|
| `eq`         | equals                  | `=`          |
| `ne`         | not equals              | `!=`         |
| `lt`         | less than               | `<`          |
| `le`         | less than equals        | `<=`         |
| `gt`         | greater than            | `>`          |
| `ge`         | greater than equals     | `>=`         |
| `co`         | contains                | `*string*`   |
| `sw`         | starts with             | `^string*`   |
| `ew`         | ends with               | `*string$`   |
| `is`         | is                      | `IS`         |
| `li`         | is like (use with %val) | `ILIKE`      |

## Further configuration

You are free to provide as many links as necessary.

**Local Dev**
set the `CUSTOM_QUICKLINKS` environment variable

**Prebuilt docker image**
Provide the `CUSTOM_QUICKLINKS` environment variable for the docker run command

```bash
  CUSTOM_QUICKLINKS='[{"href": "https://github.com/Netflix/metaflow", "label": "GitHub"}]' docker run metaflow/ui-service

```

## System Notifications for UI

You can define notifications that are broadcasted to all clients using the UI by setting an environment variable `NOTIFICATIONS` for the backend process. The value should be a _stringified_ json of the format:

```json
[
  {
    "created": 1618404534000,
    "message": "Upcoming service maintenance"
  }
]
```

You can provide as many notifications as necessary, topmost item is considered the latest.

Following attributes are supported:

| Attribute | Description                                                                       | Default value                                 |
| --------- | --------------------------------------------------------------------------------- | --------------------------------------------- |
| `id`      | Notification identifier                                                           | Generated SHA1 hash `622b3a6...` - `optional` |
| `type`    | Notification type, allowed values: `success,info,warning,danger,default`          | `info` - `optional`                           |
| `message` | Message to display (Markdown supported)                                           | `required`                                    |
| `created` | Notification created at (Epoch timestamp in milliseconds)                         | `required`                                    |
| `start`   | Schedule notification to be visible starting at (Epoch timestamp in milliseconds) | `null` - `optional`                           |
| `end`     | Schedule notification to disappear after (Epoch timestamp in milliseconds)        | `null` - `optional`                           |

Example with all the attributes:

```json
[
  {
    "id": "fixed_id_attribute",
    "type": "info",
    "message": "Upcoming service maintenance",
    "created": 1618404534000,
    "start": 1618404534000,
    "end": 1618925483000
  }
]
```
In case more intricate configuration is required for any component of the UI Service,
see [README-environment.md](docs/environment.md) for details on possible environment variables that can be tuned.
